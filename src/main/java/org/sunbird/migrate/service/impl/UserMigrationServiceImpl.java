package org.sunbird.migrate.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.common.util.PropertiesCache;
import org.sunbird.core.config.PropertiesConfig;
import org.sunbird.migrate.service.UserMigrationService;
import org.sunbird.profile.service.ProfileService;
import org.sunbird.user.service.UserUtilityService;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class UserMigrationServiceImpl implements UserMigrationService {

    private Logger log = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    CbExtServerProperties serverConfig;

    @Autowired
    PropertiesConfig propertiesConfig;

    @Autowired
    ProfileService profileService;

    @Autowired
    UserUtilityService userUtilityService;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    ObjectMapper mapper;

    @Override
    public SBApiResponse migrateUsers() {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_MIGRATION);
        StringBuilder url = new StringBuilder(propertiesConfig.getLmsServiceHost()).append(propertiesConfig.getLmsUserSearchEndPoint());
        log.info("Printing user search URL: {}", url);

        int offset = 0;
        int limit = 250;
        final int MAX_RETRIES = 3;
        int syncUserBatchSize = 10;
        int totalProcessed = 0;
        int successCount = 0;
        int failedCount = 0;
        int alreadyMigratedUsers = 0;
        boolean partialFailureOccurred = false;
        String custodianOrgName = serverConfig.getCustodianOrgName();
        String custodianOrgId = serverConfig.getCustodianOrgId();
        int MAX_OFFSET = Integer.parseInt(serverConfig.getBulkUserMigrateMaxSize());
        List<String> successfullyMigratedUsers = new ArrayList<>();
        try {
            int searchUserFailedAttemptCount = 0;
            List<Map<String, Object>> usersList = new ArrayList<>();
            while (offset < MAX_OFFSET) {
                if (searchUserFailedAttemptCount >= MAX_RETRIES) {
                    log.error("Max retry limit ({}) reached. Exiting user fetch loop.", MAX_RETRIES);
                    partialFailureOccurred = true; // mark for client awareness
                    break;
                }
                Map<String, Object> request = userSearchRequestBody(offset, limit);
                Map<String, Object> searchResponse = outboundRequestHandlerService.fetchResultUsingPost(url.toString(), request, null);

                if (MapUtils.isNotEmpty(searchResponse) && searchResponse.containsKey(Constants.RESPONSE_CODE) && Constants.OK.equalsIgnoreCase((String) searchResponse.get(Constants.RESPONSE_CODE))) {
                    searchUserFailedAttemptCount = 0;
                    Map<String, Object> result = (Map<String, Object>) searchResponse.get(Constants.RESULT);
                    Map<String, Object> responseData = (Map<String, Object>) result.get(Constants.RESPONSE);
                    List<Map<String, Object>> users = (List<Map<String, Object>>) responseData.get(Constants.CONTENT);
                    if (users.isEmpty()) {
                        log.info("Total users fetched to migrate: {}, No more users found. Exiting pagination.", usersList.size());
                        break;
                    }
                    usersList.addAll(users);
                    offset += users.size();
                } else {
                    log.error("Malformed searchResponse (missing RESPONSE_CODE): {}", searchResponse);
                    searchUserFailedAttemptCount++;
                    try {
                        Thread.sleep(1000); // 1-second delay before retrying to give the server a short break
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); // restore interrupt status
                        log.warn("Thread sleep interrupted during retry delay", ie);
                    }
                }
            }

            for (Map<String, Object> user : usersList) {
                totalProcessed++;
                String userId = (String) user.get(Constants.USER_ID);
                try {
                    boolean orgFound = false;
                    String rootOrgName = (String) user.get("rootOrgName");
                    if (rootOrgName != null) {
                        orgFound = rootOrgName.equalsIgnoreCase(custodianOrgName);
                        if (!orgFound) {
                            log.info("Organization '{}' not found for user ID '{}'. Initiating migration API call.", custodianOrgName, userId);
                            String errMsg = executeMigrateUser(getUserMigrateRequest(userId, custodianOrgName, false), null);
                            if (StringUtils.isNotEmpty(errMsg)) {
                                log.info("Migration failed for user ID '{}'. Error: {}", userId, errMsg);
                                failedCount++;
                                partialFailureOccurred = true;
                                continue; // skip rest of the steps for this user
                            }

                            SBApiResponse userPatchResponse = profileUpdateAfterNMUMigration(custodianOrgName, userId);
                            if (!userPatchResponse.getResponseCode().is2xxSuccessful()) {
                                log.info("Profile Update failed for user ID: {}", userId);
                                failedCount++;
                                partialFailureOccurred = true;
                                continue;
                            }

                            Map<String, Object> requestBody = new HashMap<String, Object>() {{
                                put(Constants.ORGANIZATION_ID, custodianOrgId);
                                put(Constants.USER_ID, userId);
                                put(Constants.ROLES, Arrays.asList(Constants.PUBLIC));
                            }};
                            Map<String, Object> roleRequest = new HashMap<String, Object>() {{
                                put("request", requestBody);
                            }};
                            String assignRoleUrl = serverConfig.getSbUrl() + serverConfig.getSbAssignRolePath();
                            Map<String, Object> assignRole = outboundRequestHandlerService.fetchResultUsingPost(assignRoleUrl, roleRequest, null);

                            if (!Constants.OK.equalsIgnoreCase((String) assignRole.get(Constants.RESPONSE_CODE))) {
                                log.info("Failed to assign role for user '{}'. Response: {}", userId, assignRole);
                                failedCount++;
                                partialFailureOccurred = true;
                                continue;
                            }

                            // All steps passed
                            successCount++;
                            successfullyMigratedUsers.add(userId);
                            log.info("Successfully migrated and updated user ID '{}'.", userId);

                        } else {
                            log.info("Organization '{}' found for user ID '{}'. No migration needed.", custodianOrgName, userId);
                            alreadyMigratedUsers++;
                        }
                    }
                } catch (Exception ex) {
                    log.error("Unexpected error while processing user '{}': {}", userId, ex.getMessage(), ex);
                    failedCount++;
                    partialFailureOccurred = true;
                }
            }

            for (int i = 0; i < successfullyMigratedUsers.size(); i += syncUserBatchSize) {
                List<String> userIds = successfullyMigratedUsers.subList(i, Math.min(i + syncUserBatchSize, successfullyMigratedUsers.size()));
                int retryCount = 0;
                boolean syncSuccessful = false;

                while (retryCount < MAX_RETRIES && !syncSuccessful) {
                    log.info("Syncing batch of user IDs: {} (Attempt {}/{})", userIds, retryCount + 1, MAX_RETRIES);
                    String errMsg = syncUserData(userIds);
                    if (StringUtils.isNotEmpty(errMsg)) {
                        retryCount++;
                        log.error("Data sync failed for batch: {}. Error: {}", userIds, errMsg);
                        if (retryCount < MAX_RETRIES) {
                            Thread.sleep(500 * (long) Math.pow(2, retryCount)); // 500ms, 1000ms, 2000ms: small delay before retry
                        }
                    } else {
                        log.info("Successfully synced user batch: {}", userIds);
                        syncSuccessful = true;
                    }
                }

                if (!syncSuccessful) {
                    log.error("Data sync permanently failed for batch after {} attempts: {}", MAX_RETRIES, userIds);
                }
                Thread.sleep(100); // small delay between batches
            }

            // Always return SUCCESS unless exception occurs
            response.setResponseCode(HttpStatus.OK);
            response.getParams().setStatus(Constants.SUCCESS);
            if (partialFailureOccurred) {
                response.getParams().setErrmsg("User migration completed with some failures. Check counts.");
            }

        } catch (Exception e) {
            log.error("Error during user migration: {}", e.getMessage(), e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
        }
        response.getResult().put("totalUsersProcessed", totalProcessed);
        response.getResult().put("usersMigratedSuccessfully", successCount);
        response.getResult().put("usersFailedToMigrate", failedCount);
        response.getResult().put("alreadyMigratedUsers", alreadyMigratedUsers);

        return response;
    }

    private void handleErrorResponse(Map<String, Object> updateResponse, SBApiResponse response) {
        // Handle error response
        if (updateResponse != null && Constants.CLIENT_ERROR.equalsIgnoreCase((String) updateResponse.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> responseParams = (Map<String, Object>) updateResponse.get(Constants.PARAMS);
            if (MapUtils.isNotEmpty(responseParams)) {
                String errorMessage = (String) responseParams.get(Constants.ERROR_MESSAGE);
                response.getParams().setErrmsg(errorMessage);
            }
            response.setResponseCode(HttpStatus.BAD_REQUEST);
        } else {
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        response.getParams().setStatus(Constants.FAILED);
        String errMsg = response.getParams().getErrmsg();
        if (StringUtils.isEmpty(errMsg)) {
            errMsg = (String) ((Map<String, Object>) updateResponse.get(Constants.PARAMS)).get(Constants.ERROR_MESSAGE);
            errMsg = PropertiesCache.getInstance().readCustomError(errMsg);
            response.getParams().setErrmsg(errMsg);
        }
        log.error(errMsg, new Exception(errMsg));
    }
    private Map<String, Object> userSearchRequestBody(int offset, int limit) {
        ZoneId zoneId = ZoneId.of("UTC");

        ZonedDateTime currentTime = ZonedDateTime.now(zoneId);

        ZonedDateTime fortyEightHoursAgo =  currentTime.minusHours(48);

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSZ");
        String formattedFortyEightHoursAgo = fortyEightHoursAgo.format(formatter);


        // Construct the request body using Map
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.PROFILE_DETAILS_PROFILE_STATUS, Constants.NOT_MY_USER);

        log.info("printing formattedFortyEightHoursAgo "+formattedFortyEightHoursAgo);

        // Create a separate HashMap for the inner filter
        Map<String, String> innerFilter = new HashMap<>();
        innerFilter.put("<=", formattedFortyEightHoursAgo);
        filters.put(Constants.PROFILE_DETAILS_UPDATEDAS_NOT_MY_USER_ON, innerFilter);
        List<String> fields = Arrays.asList("userId", "profileDetails", "organisations", "rootOrgName");

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.FILTERS, filters);
        request.put("offset", offset);
        request.put("limit", limit);
        request.put("fields", fields);

        Map<String, Object> body = new HashMap<>();
        body.put(Constants.REQUEST, request);

        return body;
    }

    private Map<String, Object> getUserMigrateRequest(String userId, String channel, boolean isSelfMigrate) {
        Map<String, Object> requestBody = new HashMap<String, Object>() {
            {
                put(Constants.USER_ID, userId);
                put(Constants.CHANNEL, channel);
                put(Constants.SOFT_DELETE_OLD_ORG, true);
                put(Constants.NOTIFY_MIGRATION, false);
                if (!isSelfMigrate) {
                    put(Constants.FORCE_MIGRATION, true);
                }
            }
        };
        Map<String, Object> request = new HashMap<String, Object>() {
            {
                put(Constants.REQUEST, requestBody);
            }
        };
        return request;
    }

    private String executeMigrateUser(Map<String, Object> request, Map<String, String> headers) {
        String errMsg = StringUtils.EMPTY;
        Map<String, Object> migrateResponse = (Map<String, Object>) outboundRequestHandlerService.fetchResultUsingPatch(
                serverConfig.getSbUrl() + serverConfig.getLmsUserMigratePath(), request, headers);
        if (migrateResponse == null
                || !Constants.OK.equalsIgnoreCase((String) migrateResponse.get(Constants.RESPONSE_CODE))) {
            errMsg = migrateResponse == null ? "Failed to migrate User."
                    : (String) ((Map<String, Object>) migrateResponse.get(Constants.PARAMS))
                    .get(Constants.ERROR_MESSAGE);
        }
        return errMsg;
    }

    private Map<String, Object> getUserExtPatchRequest(String userId, String defaultDepartment) {
        Map<String, Object> employmentDetails = new HashMap<>();
        employmentDetails.put("departmentName", defaultDepartment);

        Map<String, Object> newProfileDetails = new HashMap<>();
        newProfileDetails.put("employmentDetails", employmentDetails);

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("userId", userId);
        requestBody.put("profileDetails", newProfileDetails);

        return new HashMap<String, Object>() {{
            put("request", requestBody);
        }};

    }

    private SBApiResponse profileUpdateAfterNMUMigration(String defaultDepartment, String userId) {
        SBApiResponse response = new SBApiResponse(Constants.ORG_PROFILE_UPDATE);

        try {
            String errMsg = "";
            Map<String, Object> userData = getUserDetailsForId(userId);
            if (ObjectUtils.isEmpty(userData)) {
                response.getParams().setErrmsg(String.format("Failed to get User record from DB. UserId: %s", userId));
                return response;
            }

            Map<String, Object> updateDBRequest = new HashMap<>();
            String profileDetailsStr = (String) userData.get(Constants.PROFILE_DETAILS_LOWER);
            if (StringUtils.isEmpty(profileDetailsStr)) {
                response.getParams().setErrmsg("ProfileDetails is null for User.");
                return response;
            }
            try {
                Map<String, Object> existingProfileDetails = mapper.readValue(profileDetailsStr,
                        new TypeReference<Map<String, Object>>() {
                        });
                existingProfileDetails.remove(Constants.UPDATE_AS_NOT_MY_USER);
                Map<String, Object> employmentDetails = null;
                if (existingProfileDetails.containsKey(Constants.EMPLOYMENT_DETAILS)) {
                    employmentDetails = (Map<String, Object>) existingProfileDetails.get(Constants.EMPLOYMENT_DETAILS);
                } else {
                    employmentDetails = new HashMap<>();
                }
                employmentDetails.put(Constants.DEPARTMENTNAME, defaultDepartment);
                employmentDetails.put(Constants.DEPARTMENT_ID, (String) userData.get(Constants.ROOT_ORG_ID));
                existingProfileDetails.put(Constants.EMPLOYMENT_DETAILS, employmentDetails);

                Map<String, Object> professionalDetail = null;
                if (existingProfileDetails.containsKey(Constants.PROFESSIONAL_DETAILS)
                        && !ObjectUtils.isEmpty(existingProfileDetails.get(Constants.PROFESSIONAL_DETAILS))) {
                    professionalDetail = ((List<Map<String, Object>>) existingProfileDetails.get(Constants.PROFESSIONAL_DETAILS))
                            .get(0);
                } else {
                    professionalDetail = new HashMap<>();
                    professionalDetail.put(Constants.OSID, UUID.randomUUID().toString());
                }

                professionalDetail.put(Constants.NAME, defaultDepartment);
                professionalDetail.put(Constants.ID, (String) userData.get(Constants.ROOT_ORG_ID));
                existingProfileDetails.put(Constants.PROFESSIONAL_DETAILS, Arrays.asList(professionalDetail));

                updateDBRequest.put(Constants.PROFILE_DETAILS_LOWER, mapper.writeValueAsString(existingProfileDetails));
            } catch (Exception e) {
                errMsg = String.format("Failed to parse profileDetails object for userId: %s. Exception: ", userId);
                log.error(errMsg, e);
                response.getParams().setErrmsg(errMsg);
                return response;
            }

            Map<String, Object> compositeKey = new HashMap<String, Object>();
            compositeKey.put(Constants.ID, userId);
            Map<String, Object> updateDBResponse = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_USER, updateDBRequest, compositeKey);
            if (updateDBResponse != null
                    && !Constants.SUCCESS.equalsIgnoreCase((String) updateDBResponse.get(Constants.RESPONSE))) {
                errMsg = String.format("Failed to update profileDetails for UserId : %s", userId);
                response.getParams().setErrmsg(errMsg);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            } else {
                log.info("Successfully processed profile update for userId: {}", userId);
                response.setResponseCode(HttpStatus.OK);
                response.getResult().put(Constants.RESPONSE, Constants.SUCCESS);
                response.getParams().setStatus(Constants.SUCCESS);
            }
        } catch (Exception e) {
            log.error("Failed to process profile update. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private Map<String, Object> getUserDetailsForId(String userId) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ID, userId);
        List<Map<String, Object>> userList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER, request, null);
        if (CollectionUtils.isNotEmpty(userList)) {
            return userList.get(0);
        } else {
            return MapUtils.EMPTY_MAP;
        }
    }

    private String syncUserData(List<String> userIds) {
        String errMsg = null;
        try {
            Map<String, Object> requestBody = new HashMap<>();
            Map<String, Object> request = new HashMap<>();
            request.put(Constants.OPERATION_TYPE, Constants.SYNC);
            request.put(Constants.OBJECT_IDS, userIds);
            request.put(Constants.OBJECT_TYPE, Constants.USER);
            requestBody.put(Constants.REQUEST, request);

            Map<String, Object> syncDataResp = (Map<String, Object>) outboundRequestHandlerService.fetchResultUsingPost(
                    serverConfig.getSbUrl() + serverConfig.getLmsDataSyncPath(), requestBody, MapUtils.EMPTY_MAP);

            if (syncDataResp == null
                    || !Constants.OK.equalsIgnoreCase((String) syncDataResp.get(Constants.RESPONSE_CODE))) {
                errMsg = "Failed to call Data Sync after updating Profile for Users: " + userIds;
                log.error("syncUserData failed: response={}", syncDataResp);
            }
        } catch (Exception e) {
            errMsg = "Exception during syncUserData for Users: " + userIds + ". Error: " + e.getMessage();
            log.error(errMsg, e);
        }
        return errMsg;
    }
}
