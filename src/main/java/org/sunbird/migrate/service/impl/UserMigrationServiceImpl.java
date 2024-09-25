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

    @Override
    public SBApiResponse migrateUsers() {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_MIGRATION);
        StringBuilder url = new StringBuilder(propertiesConfig.getLmsServiceHost()).append(propertiesConfig.getLmsUserSearchEndPoint());
        log.info("Printing user search URL: {}", url);

        int offset = 0;
        int limit = 250;
        boolean allOperationsSuccessful = true;
        String custodianOrgName = serverConfig.getCustodianOrgName();
        String custodianOrgId   = serverConfig.getCustodianOrgId();
        try {
            while (true) {
                Map<String, Object> request = userSearchRequestBody(offset, limit);
                Map<String, Object> updateResponse = outboundRequestHandlerService.fetchResultUsingPost(url.toString(), request, null);

                if (Constants.OK.equalsIgnoreCase((String) updateResponse.get(Constants.RESPONSE_CODE))) {
                    Map<String, Object> result = (Map<String, Object>) updateResponse.get(Constants.RESULT);
                    Map<String, Object> responseData = (Map<String, Object>) result.get(Constants.RESPONSE);
                    List<Map<String, Object>> users = (List<Map<String, Object>>) responseData.get(Constants.CONTENT);

                    if (users.isEmpty()) {
                        log.info("No more users found. Exiting pagination.");
                        break;
                    }

                    for (Map<String, Object> user : users) {
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
                                    allOperationsSuccessful = false;
                                } else {
                                    log.info("Successfully migrated user ID '{}'.", userId);
                                    Map<String, Object> userPatchRequest = getUserExtPatchRequest(userId, custodianOrgName);
                                    SBApiResponse userPatchResponse = profileService.profileMDOAdminUpdate(userPatchRequest, null, serverConfig.getSbApiKey(), null);
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
                                        } else {
                                            String assignRoleErrorMessage = (String) assignRole.get(Constants.ERROR_MESSAGE);
                                            log.info("Failed to assign 'PUBLIC' role for user ID '{}'. Response: {}. Error: {}", userId, assignRole, assignRoleErrorMessage);
                                            allOperationsSuccessful = false;
                                        }
                                    } else {
                                        log.info("Patch failed for user ID '{}'. Response: {}", userId, userPatchResponse);
                                        allOperationsSuccessful = false;
                                    }
                                }
                            } else {
                                log.info("Organization '{}' found for user ID '{}'. No migration needed.", custodianOrgName, userId);
                            }
                        }
                    }

                    offset += users.size();
                } else {
                    handleErrorResponse(updateResponse, response);
                    return response;
                }
            }

            if (allOperationsSuccessful) {
                response.setResponseCode(HttpStatus.OK);
                response.getParams().setStatus(Constants.SUCCESS);
            } else {
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                response.getParams().setStatus(Constants.FAILED);
            }

        } catch (Exception e) {
            log.error("Error during user migration: {}", e.getMessage(), e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
        }

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

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSZ");
        String formattedCurrentTime = currentTime.format(formatter);


        ZonedDateTime yesterdayAtOneAM = currentTime
                .minusDays(1) // Subtract one day to get yesterday
                .toLocalDate()
                .atStartOfDay(zoneId);

        // Format yesterday's time
        String formattedYesterdayAtOneAM = yesterdayAtOneAM.format(formatter);

        // Construct the request body using Map
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.PROFILE_DETAILS_PROFILE_STATUS, Constants.NOT_MY_USER);

        log.info("printing formattedYesterdayAtOneAM "+formattedYesterdayAtOneAM);
        log.info("printing formattedCurrentTime "+formattedCurrentTime);

        // Create a separate HashMap for the inner filter
        Map<String, String> innerFilter = new HashMap<>();
        innerFilter.put("<=", formattedCurrentTime);
        innerFilter.put(">=", formattedYesterdayAtOneAM);
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
}
