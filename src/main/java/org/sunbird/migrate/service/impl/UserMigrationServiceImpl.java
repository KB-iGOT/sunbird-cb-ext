package org.sunbird.migrate.service.impl;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.recycler.Recycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
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

    @Override
    public SBApiResponse migrateUsers() {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_MIGRATION);
        StringBuilder url = new StringBuilder(propertiesConfig.getLmsServiceHost()).append(propertiesConfig.getLmsUserSearchEndPoint());
        log.info("Printing user search URL: {}", url);

        int offset = 0;
        int limit = 250;
        final int MAX_RETRIES = 3;
        int totalProcessed = 0;
        int successCount = 0;
        int failedCount = 0;
        int alreadyMigratedUsers = 0;
        boolean partialFailureOccurred = false;
        String custodianOrgName = serverConfig.getCustodianOrgName();
        String custodianOrgId = serverConfig.getCustodianOrgId();
        try {
            int searchUserFailedAttemptCount = 0;
            while (true) {
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
                        log.info("No more users found. Exiting pagination.");
                        break;
                    }

                    for (Map<String, Object> user : users) {
                        totalProcessed++;
                        String userId = (String) user.get(Constants.USER_ID);
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
                                } else {
                                    log.info("Successfully migrated user ID '{}'.", userId);
                                    SBApiResponse userPatchResponse = profileUpdateAfterNMUMigration(custodianOrgName, serverConfig.getSbApiKey(), userId);
                                    log.info("userPatchResponse for user ID '{}'.", userPatchResponse);
                                    if (userPatchResponse.getResponseCode().is2xxSuccessful()) {
                                        log.info("Successfully patched user ID '{}'. Response: {}", userId, userPatchResponse);
                                        Map<String, Object> requestBody = new HashMap<String, Object>() {{
                                            put(Constants.ORGANIZATION_ID, custodianOrgId);
                                            put(Constants.USER_ID, userId);
                                            put(Constants.ROLES, Arrays.asList(Constants.PUBLIC));
                                        }};
                                        Map<String, Object> roleRequest = new HashMap<String, Object>() {{
                                            put("request", requestBody);
                                        }};
                                        StringBuilder assignRoleUrl = new StringBuilder(serverConfig.getSbUrl()).append(serverConfig.getSbAssignRolePath());
                                        log.info("printing assignRoleUrl: {}", assignRoleUrl);
                                        Map<String, Object> assignRole = outboundRequestHandlerService.fetchResultUsingPost(assignRoleUrl.toString(), roleRequest, null);

                                        if (Constants.OK.equalsIgnoreCase((String) assignRole.get(Constants.RESPONSE_CODE))) {
                                            log.info("Successfully assigned public role for user ID '{}'. Response: {}", userId, assignRole);
                                            successCount++;
                                        } else {
                                            String assignRoleErrorMessage = (String) assignRole.get(Constants.ERROR_MESSAGE);
                                            log.info("Failed to assign 'PUBLIC' role for user ID '{}'. Response: {}. Error: {}", userId, assignRole, assignRoleErrorMessage);
                                            failedCount++;
                                            partialFailureOccurred = true;
                                        }
                                    } else {
                                        log.info("Patch failed for user ID '{}'. Response: {}", userId, userPatchResponse);
                                        failedCount++;
                                        partialFailureOccurred = true;
                                    }
                                }
                            } else {
                                log.info("Organization '{}' found for user ID '{}'. No migration needed.", custodianOrgName, userId);
                                alreadyMigratedUsers++;
                            }
                        }
                    }

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

    private SBApiResponse profileUpdateAfterNMUMigration(String defaultDepartment, String authToken, String userId) {
        SBApiResponse response = new SBApiResponse(Constants.ORG_PROFILE_UPDATE);

        try {
            Map<String, Object> responseMap = userUtilityService.getUsersReadData(userId, StringUtils.EMPTY,
                    StringUtils.EMPTY);
            Map<String, Object> existingProfileDetails = (Map<String, Object>) responseMap.get(Constants.PROFILE_DETAILS);
            existingProfileDetails.remove(Constants.UPDATE_AS_NOT_MY_USER);

            Map<String, Object> employmentDetails = new HashMap<>();
            employmentDetails.put("departmentName", defaultDepartment);
            existingProfileDetails.put(Constants.EMPLOYMENT_DETAILS, employmentDetails);

            HashMap<String, String> headerValue = new HashMap<>();
            headerValue.put(Constants.AUTH_TOKEN, authToken);
            headerValue.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);

            String updatedUrl = serverConfig.getSbUrl() + serverConfig.getLmsUserUpdatePrivatePath();
            Map<String, Object> request = new HashMap<>();
            request.put(Constants.USER_ID, userId);
            request.put(Constants.PROFILE_DETAILS, existingProfileDetails);
            Map<String, Object> updateRequest = new HashMap<>();
            updateRequest.put(Constants.REQUEST, request);
            Map<String, Object> updateResponse = outboundRequestHandlerService.fetchResultUsingPatch(updatedUrl, updateRequest, headerValue);

            if (Constants.OK.equalsIgnoreCase((String) updateResponse.get(Constants.RESPONSE_CODE))) {
                log.info("Successfully processed profile update for userId: {}", userId);
                response.setResponseCode(HttpStatus.OK);
                response.getResult().put(Constants.RESPONSE, Constants.SUCCESS);
                response.getParams().setStatus(Constants.SUCCESS);
            } else {
                if (Constants.CLIENT_ERROR.equalsIgnoreCase((String) updateResponse.get(Constants.RESPONSE_CODE))) {
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
                return response;
            }
        } catch (Exception e) {
            log.error("Failed to process profile update. Exception: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErr(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }
}
