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
import org.sunbird.common.util.PropertiesCache;
import org.sunbird.core.config.PropertiesConfig;
import org.sunbird.migrate.service.UserMigrationService;

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

    @Override
    public SBApiResponse migrateUsers() {
        SBApiResponse response = new SBApiResponse(Constants.API_USER_MIGRATION);
        StringBuilder url = new StringBuilder(propertiesConfig.getLmsServiceHost()).append(propertiesConfig.getLmsUserSearchEndPoint());
        log.info("printing user search Url: {}", url);

      try {
          Map<String, Object> request = userSearchRequestBody();
          Map<String, Object> updateResponse = outboundRequestHandlerService.fetchResultUsingPatch(url.toString(), request, null);

          if (Constants.OK.equalsIgnoreCase((String) updateResponse.get(Constants.RESPONSE_CODE))) {
              Map<String, Object> result = (Map<String, Object>) updateResponse.get(Constants.RESULT);
              Map<String, Object> responseData = (Map<String, Object>) result.get(Constants.RESPONSE);
              List<Map<String, Object>> users = (List<Map<String, Object>>) responseData.get(Constants.CONTENT);
              String targetOrgName = Constants.IGOT;
              boolean orgFound = false;
              boolean allOperationsSuccessful = true;

              for (Map<String, Object> user : users) {
                  List<Map<String, Object>> organisations = (List<Map<String, Object>>) user.get(Constants.ORGANISATIONS);
                  String userId = (String) user.get(Constants.USER_ID);
                  if (organisations != null) {
                      orgFound = organisations.stream()
                              .anyMatch(organisation -> targetOrgName.equalsIgnoreCase((String) organisation.get(Constants.ORG_NAME)));
                      if (!orgFound) {
                          log.info("Organization '{}' not found for user ID '{}'. Initiating migration API call.", targetOrgName, userId);
                          String errMsg = executeMigrateUser(getUserMigrateRequest(userId, targetOrgName, false), null);
                          if (StringUtils.isNotEmpty(errMsg)) {
                              log.info("Migration failed for user ID '{}'. Error: {}", userId, errMsg);
                              allOperationsSuccessful = false;
                          } else {
                              log.info("Successfully migrated user ID '{}'.", userId);
                              Map<String, Object> profileDetails = (Map<String, Object>) user.get(Constants.PROFILE_DETAILS);
                              Map<String, String> headerValues = new HashMap<>();
                              headerValues.put(Constants.AUTH_TOKEN, "Bearer " + serverConfig.getSbApiKey());
                              headerValues.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
                              StringBuilder userPatchUrl = new StringBuilder(serverConfig.getCbExtHost())
                                      .append(serverConfig.getUserPatch());
                              log.info("printing userPatchUrl: {}", userPatchUrl);

                              Map<String, Object> userPatchResponse = outboundRequestHandlerService.fetchResultUsingPost(userPatchUrl.toString(), getUserExtPatchRequest(userId, profileDetails, targetOrgName), headerValues);
                              if (Constants.OK.equalsIgnoreCase((String) userPatchResponse.get(Constants.RESPONSE_CODE))) {
                                  log.info("Successfully patched user ID '{}'. Response: {}", userId, userPatchResponse);
                                  Map<String, Object> requestBody = new HashMap<String, Object>() {{
                                      put(Constants.ORGANIZATION_ID, serverConfig.getFallbackDepartmentId());
                                      put(Constants.USER_ID, userId);
                                      put(Constants.ROLES, Arrays.asList(Constants.PUBLIC));
                                  }};
                                  StringBuilder assignRoleUrl = new StringBuilder(serverConfig.getSbUrl()).append(serverConfig.getSbAssignRolePath());
                                  log.info("printing assignRoleUrl: {}", assignRoleUrl);
                                  Map<String, Object> assignRole = outboundRequestHandlerService.fetchResultUsingPost(assignRoleUrl.toString(), requestBody, null);

                                  if (Constants.OK.equalsIgnoreCase((String) assignRole.get(Constants.RESPONSE_CODE))) {
                                      log.info("Successfully assigned public role for user ID '{}'. Response: {}", userId, assignRole);
                                  } else {
                                      String assignRoleErrorMessage = (String) assignRole.get(Constants.ERROR_MESSAGE);
                                      log.info("Failed to assign 'PUBLIC' role for user ID '{}'. Response: {}. Error: {}", userId, assignRole, assignRoleErrorMessage);
                                      allOperationsSuccessful = false; // Update success flag
                                  }
                              } else {
                                  log.info("Patch failed for user ID '{}'. Response: {}", userId, userPatchResponse);
                                  allOperationsSuccessful = false; // Update success flag
                              }
                          }
                      } else {
                          log.info("Organization '{}' found for user ID '{}'. No migration needed.", targetOrgName, userId);
                      }
                  }
                  orgFound = false;
              }

              if (allOperationsSuccessful) {
                  response.setResponseCode(HttpStatus.OK);
                  response.getParams().setStatus(Constants.SUCCESS);
              } else {
                  response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                  response.getParams().setStatus(Constants.FAILED);
              }

          } else {
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
              return response;
          }
      } catch (Exception e) {
          log.error("Error during user migration: {}", e.getMessage(), e);
          response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
          response.getParams().setStatus(Constants.FAILED);
          response.getParams().setErrmsg(e.getMessage());
      }

        return response;
    }


    private Map<String, Object> userSearchRequestBody() {
        ZoneId zoneId = ZoneId.of("UTC");

        // Get the current time in UTC
        ZonedDateTime currentTime = ZonedDateTime.now(zoneId);

        // Format the current time to the desired string format
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSS'+'0000");
        String formattedCurrentTime = currentTime.format(formatter);

        // Get yesterday's date at 1 AM in UTC
        ZonedDateTime yesterdayAtOneAM = currentTime
                .minusDays(1) // Subtract one day to get yesterday
                .withHour(1)  // Set the hour to 1 AM
                .withMinute(0)
                .withSecond(0)
                .withNano(0); // No need to set zone again as it's already in UTC

        // Format yesterday's time
        String formattedYesterdayAtOneAM = yesterdayAtOneAM.format(formatter);

        // Construct the request body using Map
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.PROFILE_DETAILS_PROFILE_STATUS, Constants.NOT_MY_USER);

        // Create a separate HashMap for the inner filter
        Map<String, String> innerFilter = new HashMap<>();
        innerFilter.put("<=", formattedCurrentTime);
        innerFilter.put(">=", formattedYesterdayAtOneAM);
        filters.put(Constants.PROFILE_DETAILS_UPDATEDAS_NOT_MY_USER_ON, innerFilter);

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.FILTERS, filters);

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


    private Map<String, Object> getUserExtPatchRequest(String userId, Map<String, Object> profileDetails, String defaultDepartment) {

        Map<String, Object> employmentDetails = (Map<String, Object>) profileDetails.get("employmentDetails");
        if (employmentDetails == null) {
            employmentDetails = new HashMap<>();
        }

        employmentDetails.put("departmentName", defaultDepartment);

        Map<String, Object> requestBody = new HashMap<String, Object>() {{
            put("userId", userId);
            put("profileDetails", profileDetails);
        }};

        profileDetails.put("employmentDetails", employmentDetails);

        return new HashMap<String, Object>() {{
            put("request", requestBody);
        }};
    }


}
