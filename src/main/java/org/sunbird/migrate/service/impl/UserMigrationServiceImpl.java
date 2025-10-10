package org.sunbird.migrate.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.common.recycler.Recycler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.*;
import org.sunbird.consumer.KafkaProducer;
import org.sunbird.core.config.PropertiesConfig;
import org.sunbird.core.producer.Producer;
import org.sunbird.migrate.service.UserMigrationService;
import org.sunbird.migrate.util.OrganizationUtils;
import org.sunbird.org.util.ExcelUtil;
import org.sunbird.org.util.FrameworkUtil;
import org.sunbird.profile.service.ProfileService;
import org.sunbird.storage.service.StorageServiceImpl;
import org.sunbird.user.service.UserUtilityService;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Autowired
    ExcelUtil excelUtil;

    @Autowired
    OrganizationUtils organizationUtils;

    @Autowired AccessTokenValidator accessTokenValidator;

    @Autowired
    FrameworkUtil frameworkUtil;

    @Autowired
    RedisCacheMgr redisCacheMgr;

    @Autowired StorageServiceImpl storageService;

    @Autowired
    Producer kafkaProducer;

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
                                    SBApiResponse userPatchResponse = profileUpdateAfterNMUMigration(custodianOrgName, userId);
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
                syncUserData(userId);
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

    private String syncUserData(String userId) {
        String errMsg = null;
        Map<String, Object> requestBody = new HashMap<String, Object>();
        Map<String, Object> request = new HashMap<String, Object>();
        request.put(Constants.OPERATION_TYPE, Constants.SYNC);
        request.put(Constants.OBJECT_IDS, Arrays.asList(userId));
        request.put(Constants.OBJECT_TYPE, Constants.USER);
        requestBody.put(Constants.REQUEST, request);

        Map<String, Object> syncDataResp = (Map<String, Object>) outboundRequestHandlerService.fetchResultUsingPost(
                serverConfig.getSbUrl() + serverConfig.getLmsDataSyncPath(), requestBody, MapUtils.EMPTY_MAP);
        if (syncDataResp == null
                || !Constants.OK.equalsIgnoreCase((String) syncDataResp.get(Constants.RESPONSE_CODE))) {
            errMsg = "Failed to call Data Sync after updating Profile for User: " + userId;
        }
        return errMsg;
    }

    private void addTermToHierarchy(String orgId, Map<String, Map<String, Object>> termMap,
                                    List<Map<String, String>> hierarchyList) {
        Map<String, Object> term = termMap.get(orgId);
        if (MapUtils.isNotEmpty(term)) {
            Map<String, String> hierarchyEntry = new HashMap<>();
            hierarchyEntry.put(Constants.CHANNEL, (String) term.get(Constants.NAME));
            hierarchyEntry.put(Constants.ID, orgId);
            hierarchyEntry.put(Constants.CATEGORY, (String) term.get(Constants.CATEGORY));
            hierarchyList.add(hierarchyEntry);
        }
    }

    private String extractOrgId(Map<String, Object> term) {
        Map<String, Object> additionalProperties = (Map<String, Object>) term.get(Constants.ADDITIONAL_PROPERTIES);
        return MapUtils.isNotEmpty(additionalProperties) ? (String) additionalProperties.get(Constants.ORG_ID) : null;
    }

    public Map<String, Object> getCompleteFrameworkDataFromUtil(String frameworkId) throws Exception {
        try {
            String cacheKey = Constants.BULK_TRANSFER_FRAMEWORK_HIERARCHY_CACHE_KEY_PREFIX + frameworkId;
            String cachedDataStr = redisCacheMgr.getCache(cacheKey);
            if (StringUtils.isNotEmpty(cachedDataStr) ||
                    Constants.NULL_STRING.equalsIgnoreCase(cachedDataStr) ||
                    Constants.EMPTY_JSON_OBJECT.equals(cachedDataStr) ||
                    Constants.EMPTY_JSON_ARRAY.equals(cachedDataStr)) {
                log.info("Cache hit for getCompleteFrameworkDataFromUtil, frameworkId: {}", frameworkId);
                return mapper.readValue(cachedDataStr, Map.class);
            }
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.AUTHORIZATION, serverConfig.getSbApiKey());
            String url = serverConfig.getKmBaseHost() + serverConfig.getKmFrameWorkPath() + "/" + frameworkId;

            Map<String, Object> frameworkResponse = (Map<String, Object>) outboundRequestHandlerService.fetchUsingGetWithHeaders(url, headers);

            if (MapUtils.isNotEmpty(frameworkResponse)  && Constants.OK.equals(frameworkResponse.get(Constants.RESPONSE_CODE))) {
                log.info("Successfully received framework data for frameworkId: {}", frameworkId);
                redisCacheMgr.putCache(cacheKey, frameworkResponse, serverConfig.getBulkTransferRedisTtl());
                return frameworkResponse;
            } else {
                log.error("Framework API call failed for frameworkId: {}. Response: {}", frameworkId, frameworkResponse);
                return null;
            }
        } catch (Exception e) {
            log.error("Error calling framework API for frameworkId: {}", frameworkId, e);
            throw e;
        }
    }

    public List<Map<String, String>> fetchCompleteFrameworkHierarchy(String frameworkId) throws Exception {
        Map<String, Object> frameworkData = getCompleteFrameworkDataFromUtil(frameworkId);
        List<Map<String, String>> hierarchyList = new ArrayList<>();

        if (MapUtils.isNotEmpty(frameworkData) && frameworkData.containsKey(Constants.RESULT)) {
            Map<String, Object> result = (Map<String, Object>) frameworkData.get(Constants.RESULT);
            if (MapUtils.isNotEmpty(result) && result.containsKey(Constants.FRAMEWORK)) {
                Map<String, Object> framework = (Map<String, Object>) result.get(Constants.FRAMEWORK);
                if (MapUtils.isNotEmpty(framework)) {
                    // Build complete term maps
                    Map<String, Map<String, Object>> termMap = new HashMap<>();
                    Map<String, List<String>> parentChildMap = new HashMap<>();
                    buildCompleteTermMaps(framework, termMap, parentChildMap);

                    // Add all terms to hierarchy
                    for (String orgId : termMap.keySet()) {
                        addTermToHierarchy(orgId, termMap, hierarchyList);
                    }
                }
            }
        }
        return hierarchyList;
    }

    private List<Map<String, String>> fetchTokenOrgDescendantHierarchy(String frameworkId, String tokenOrg) throws Exception {
        Map<String, Object> frameworkData = getCompleteFrameworkDataFromUtil(frameworkId);
        List<Map<String, String>> hierarchyList = new ArrayList<>();

        if (MapUtils.isNotEmpty(frameworkData) && frameworkData.containsKey(Constants.RESULT)) {
            Map<String, Object> result = (Map<String, Object>) frameworkData.get(Constants.RESULT);
            if (MapUtils.isNotEmpty(result) && result.containsKey(Constants.FRAMEWORK)) {
                Map<String, Object> framework = (Map<String, Object>) result.get(Constants.FRAMEWORK);
                if (MapUtils.isNotEmpty(framework)) {
                    Map<String, Map<String, Object>> termMap = new HashMap<>();
                    Map<String, List<String>> parentChildMap = new HashMap<>();
                    buildCompleteTermMaps(framework, termMap, parentChildMap);
                    if (termMap.containsKey(tokenOrg)) {
                        log.info("Found token org {} in framework. Building complete descendant hierarchy.", tokenOrg);
                        addTermToHierarchy(tokenOrg, termMap, hierarchyList);
                        Set<String> allDescendants = getAllDescendants(tokenOrg, parentChildMap, new HashSet<>());
                        for (String descendantOrgId : allDescendants) {
                            addTermToHierarchy(descendantOrgId, termMap, hierarchyList);
                        }
                    } else {
                        log.warn("Token org {} not found in framework {}. Returning empty hierarchy.", tokenOrg, frameworkId);
                    }
                }
            }
        }
        return hierarchyList;
    }

    private void buildCompleteTermMaps(Map<String, Object> framework,
                                       Map<String, Map<String, Object>> termMap,
                                       Map<String, List<String>> parentChildMap) {
        List<Map<String, Object>> categories = (List<Map<String, Object>>) framework.get(Constants.CATEGORIES);

        if (CollectionUtils.isNotEmpty(categories)) {
            for (Map<String, Object> category : categories) {
                List<Map<String, Object>> terms = (List<Map<String, Object>>) category.get(Constants.TERMS);
                if (terms != null) {
                    for (Map<String, Object> term : terms) {
                        String termOrgId = extractOrgId(term);
                        if (StringUtils.isNotEmpty(termOrgId)) {
                            termMap.put(termOrgId, term);

                            // Build parent-child relationships from associations
                            List<Map<String, Object>> associations = (List<Map<String, Object>>) term.get(Constants.ASSOCIATIONS);
                            if (CollectionUtils.isNotEmpty(associations)) {
                                List<String> children = new ArrayList<>();
                                for (Map<String, Object> association : associations) {
                                    String childOrgId = extractOrgId(association);
                                    if (StringUtils.isNotEmpty(childOrgId)) {
                                        children.add(childOrgId);
                                    }
                                }
                                if (!children.isEmpty()) {
                                    parentChildMap.put(termOrgId, children);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private Set<String> getAllDescendants(String parentOrgId,
                                          Map<String, List<String>> parentChildMap,
                                          Set<String> visited) {
        Set<String> descendants = new HashSet<>();

        if (visited.contains(parentOrgId)) {
            return descendants;
        }
        visited.add(parentOrgId);

        List<String> directChildren = parentChildMap.get(parentOrgId);
        if (CollectionUtils.isNotEmpty(directChildren)) {
            for (String childOrgId : directChildren) {
                descendants.add(childOrgId);
                descendants.addAll(getAllDescendants(childOrgId, parentChildMap, visited));
            }
        }

        return descendants;
    }

    public ResponseEntity<ByteArrayResource> downloadBulkTransferSampleFile(String rootOrgId, String userAuthToken, String orgHierarchyFrameworkId) throws IOException {

        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, rootOrgId);
        List<Map<String, Object>> orgDetailsList = cassandraOperation.getRecordsByProperties(Constants.DATABASE,
                Constants.ORGANISATION, propertyMap, null);

        if (CollectionUtils.isEmpty(orgDetailsList)) {
            log.error("Organization not found for rootOrgId: {}", rootOrgId);
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Map<String, Object> orgRecord = orgDetailsList.get(0);
        String sbOrgType = (String) orgRecord.get(serverConfig.getOrgTypeFieldName());
        if (StringUtils.isEmpty(sbOrgType)) {
            log.error("ERR_ORG_TYPE_NOT_FOUND: Organization type not found. Cannot determine access permissions for rootOrgId {}", rootOrgId);
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }

        if (!Arrays.asList(Constants.SPV_LOWER_CASE, Constants.MINISTRY, Constants.STATE)
                .contains(sbOrgType.toLowerCase())) {
            log.error("ERR_BULK_TRANSFER_ACCESS_DENIED: Only SPV and MDO (Ministry/State) organizations can access the Bulk Transfer feature. OrgType: {}", sbOrgType);
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Map<String, Object> payload = accessTokenValidator.extractTokenPayload(userAuthToken);
        List<String> allowedRoles = serverConfig.getBulkTransferAuthorizedRoles();
        List<String> userRoles = (List<String>) payload.get(Constants.USER_ROLES_KEY);
        if ((Constants.MINISTRY.equalsIgnoreCase(sbOrgType) ||
                Constants.STATE.equalsIgnoreCase(sbOrgType)) &&
                userRoles.stream().noneMatch(allowedRoles::contains)) {
            log.error("ERR_BULK_TRANSFER_ROLE_DENIED: Only MDO Admins and Leaders can access the Bulk Transfer feature. UserRoles: {}", userRoles);
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }

        Boolean hasRole = false;
        for (String role : userRoles) {
            for (String allowedRole : allowedRoles) {
                if (role != null && allowedRole != null &&
                        role.trim().equalsIgnoreCase(allowedRole.trim())) {
                    hasRole = true;
                    break;
                }
            }
            if (hasRole) {
                break;
            }
        }

        if (!hasRole) {
            throw new AccessDeniedException(Constants.FILE_DOWNLOAD_PERMISSION_DENIED);
        }

        String tokenOrg = (String) payload.get(Constants.ORG);

        Workbook workbook = new XSSFWorkbook();
        try {
            Sheet referenceSheet = createReferenceSheet(workbook);
            Sheet masterDataSheet = createMasterDataSheet(workbook);

            List<Map<String, String>> orgList = getOrgList(orgHierarchyFrameworkId, tokenOrg);
            Map<String, Map<String, String>> uniqueOrgs = getUniqueOrgs(orgList);

            int rowIdx = populateMasterDataSheet(masterDataSheet, uniqueOrgs);
            setupReferenceSheet(referenceSheet, masterDataSheet, rowIdx);

            ByteArrayResource resource = writeWorkbookToResource(workbook);

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverConfig.getBulkTransferUserFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            log.error("Error while generating bulk transfer sample file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            workbook.close();
        }
    }

    private Sheet createReferenceSheet(Workbook workbook) {
        Sheet referenceSheet = workbook.createSheet(
                WorkbookUtil.createSafeSheetName(serverConfig.getBulkTransferUserReferenceSheetName()));
        excelUtil.createHeaderRow(referenceSheet, serverConfig.getBulkTransferUserReferenceHeaders());
        return referenceSheet;
    }

    private Sheet createMasterDataSheet(Workbook workbook) {
        Sheet masterDataSheet = workbook.createSheet(
                WorkbookUtil.createSafeSheetName(serverConfig.getBulkTransferUserMasterDataSheetName()));
        excelUtil.createHeaderRow(masterDataSheet, serverConfig.getBulkTransferUserMasterDataHeaders());
        return masterDataSheet;
    }

    private List<Map<String, String>> getOrgList(String orgHierarchyFrameworkId, String tokenOrg) throws Exception {
        String baseFrameworkOrgId = orgHierarchyFrameworkId.split(Constants.ORG_HIERARCHY_SUFFIX)[0];
        log.info("Framework processing - baseFrameworkOrgId: {}, orgHierarchyFrameworkId: {}, tokenOrg: {}",
                baseFrameworkOrgId, orgHierarchyFrameworkId, tokenOrg);
        if (tokenOrg != null && tokenOrg.equals(baseFrameworkOrgId)) {
            return fetchCompleteFrameworkHierarchy(orgHierarchyFrameworkId);
        } else {
            return fetchTokenOrgDescendantHierarchy(orgHierarchyFrameworkId, tokenOrg);
        }
    }

    private Map<String, Map<String, String>> getUniqueOrgs(List<Map<String, String>> orgList) {
        Map<String, Map<String, String>> uniqueOrgs = new HashMap<>();
        for (Map<String, String> org : orgList) {
            String orgId = org.get(Constants.ID);
            if (StringUtils.isNotEmpty(orgId) && !uniqueOrgs.containsKey(orgId)) {
                uniqueOrgs.put(orgId, org);
            }
        }
        return uniqueOrgs;
    }

    private int populateMasterDataSheet(Sheet masterDataSheet, Map<String, Map<String, String>> uniqueOrgs) {
        int rowIdx = 1;
        for (Map<String, String> org : uniqueOrgs.values()) {
            Row row = masterDataSheet.createRow(rowIdx++);
            String orgChannel = org.get(Constants.CHANNEL) != null ? org.get(Constants.CHANNEL) : "";
            String orgId = org.get(Constants.ID) != null ? org.get(Constants.ID) : "";
            row.createCell(0).setCellValue(orgChannel);
            row.createCell(1).setCellValue(orgId);
            row.createCell(2).setCellValue("");
            row.createCell(3).setCellValue(orgChannel + " (" + orgId + ")");
            row.createCell(4).setCellValue("");
        }
        excelUtil.makeSheetReadOnly(masterDataSheet);
        masterDataSheet.setColumnHidden(0, true);
        masterDataSheet.setColumnHidden(1, true);
        masterDataSheet.setColumnHidden(2, true);
        masterDataSheet.setColumnHidden(4, true);
        excelUtil.setColumnWidths(masterDataSheet);
        return rowIdx;
    }

    private void setupReferenceSheet(Sheet referenceSheet, Sheet masterDataSheet, int rowIdx) {
        DataValidationHelper helper = referenceSheet.getDataValidationHelper();

        CellRangeAddressList currentOrgRange = new CellRangeAddressList(1, serverConfig.getBulkUserTransferMaxRows(), 1, 1);
        DataValidationConstraint currentOrgConstraint = helper.createFormulaListConstraint(
                masterDataSheet.getSheetName() + "!$D$2:$D$" + rowIdx);
        DataValidation currentOrgValidation = helper.createValidation(currentOrgConstraint, currentOrgRange);
        currentOrgValidation.setShowErrorBox(true);
        currentOrgValidation.setErrorStyle(DataValidation.ErrorStyle.STOP);
        currentOrgValidation.createErrorBox("Invalid Selection", "Please select a valid organisation from the dropdown.");
        referenceSheet.addValidationData(currentOrgValidation);

        CellRangeAddressList targetOrgRange = new CellRangeAddressList(1, serverConfig.getBulkUserTransferMaxRows(), 2, 2);
        DataValidationConstraint targetOrgConstraint = helper.createFormulaListConstraint(
                masterDataSheet.getSheetName() + "!$D$2:$D$" + rowIdx);
        DataValidation targetOrgValidation = helper.createValidation(targetOrgConstraint, targetOrgRange);
        targetOrgValidation.setShowErrorBox(true);
        targetOrgValidation.setErrorStyle(DataValidation.ErrorStyle.STOP);
        targetOrgValidation.createErrorBox("Invalid Selection", "Please select a valid organisation from the dropdown.");
        referenceSheet.addValidationData(targetOrgValidation);

        int validationColumnIndex = 4;
        for (int rowNum = 1; rowNum <= serverConfig.getBulkUserTransferMaxRows(); rowNum++) {
            Row row = referenceSheet.getRow(rowNum);
            if (row == null) {
                row = referenceSheet.createRow(rowNum);
            }
            Cell validationCell = row.createCell(validationColumnIndex);
            validationCell.setCellFormula("IF(AND(B" + (rowNum + 1) + "<>\"\",C" + (rowNum + 1) + "<>\"\",B" + (rowNum + 1) + "=C" + (rowNum + 1) + "),\"SAME_ORG\",\"VALID\")");
        }

        excelUtil.addSameOrgHighlighting(referenceSheet, 1, serverConfig.getBulkUserTransferMaxRows(), 4, 1, 2);
        referenceSheet.setColumnHidden(validationColumnIndex, true);

        excelUtil.addDuplicateHighlighting(referenceSheet, 1, serverConfig.getBulkUserTransferMaxRows(), 0, serverConfig.getBulkTransferUserReferenceHeaders().length - 1);

        CellRangeAddressList notificationRange = new CellRangeAddressList(1, serverConfig.getBulkUserTransferMaxRows(), 3, 3);
        DataValidationConstraint notificationConstraint = helper.createExplicitListConstraint(new String[]{"true", "false"});
        DataValidation notificationValidation = helper.createValidation(notificationConstraint, notificationRange);
        notificationValidation.setShowErrorBox(true);
        notificationValidation.setErrorStyle(DataValidation.ErrorStyle.STOP);
        notificationValidation.createErrorBox("Invalid Notification", "Please select either 'true' or 'false'.");
        referenceSheet.addValidationData(notificationValidation);

        excelUtil.addDuplicateHighlighting(referenceSheet, 1, serverConfig.getBulkUserTransferMaxRows(), 0, serverConfig.getBulkTransferUserReferenceHeaders().length - 1);

        for (int i = 0; i < serverConfig.getBulkTransferUserReferenceHeaders().length; i++) {
            referenceSheet.autoSizeColumn(i);
        }
    }

    private ByteArrayResource writeWorkbookToResource(Workbook workbook) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        workbook.write(outputStream);
        return new ByteArrayResource(outputStream.toByteArray());
    }

    @Override
    public SBApiResponse bulkUploadUserTransfer(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId, String orgId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_BULK_TRANSFER_UPLOAD);
        try {
            Map<String, Object> tokenPayload = accessTokenValidator.extractTokenPayload(userAuthToken);
            String userId = (String) tokenPayload.get(Constants.SUB);
            if (StringUtils.isNotBlank(userId)) {
                int pos = userId.lastIndexOf(":");
                userId = userId.substring(pos + 1);
            }
            List<String> userRoles = (List<String>) tokenPayload.get(Constants.USER_ROLES_KEY);

            if (StringUtils.isBlank(userId)) {
                setErrorData(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ID, orgId);
            List<Map<String, Object>> orgDetailsList = cassandraOperation.getRecordsByProperties(Constants.DATABASE,
                    Constants.ORGANISATION, propertyMap, null);

            if (CollectionUtils.isEmpty(orgDetailsList)) {
                setErrorData(response, "Organization not found", HttpStatus.FORBIDDEN);
                return response;
            }

            Map<String, Object> orgRecord = orgDetailsList.get(0);
            String sbOrgType = (String) orgRecord.get(serverConfig.getOrgTypeFieldName());
            if (StringUtils.isEmpty(sbOrgType)) {
                setErrorData(response, "Organization type not found", HttpStatus.FORBIDDEN);
                return response;
            }

            if (!Arrays.asList(Constants.SPV_LOWER_CASE, Constants.MINISTRY, Constants.STATE)
                    .contains(sbOrgType.toLowerCase())) {
                setErrorData(response, "Only SPV and MDO (Ministry/State) organizations can access the Bulk Transfer feature",
                        HttpStatus.FORBIDDEN);
                return response;
            }

            List<String> allowedRoles = serverConfig.getBulkTransferAuthorizedRoles();
            if ((Constants.MINISTRY.equalsIgnoreCase(sbOrgType) ||
                    Constants.STATE.equalsIgnoreCase(sbOrgType)) &&
                    userRoles.stream().noneMatch(allowedRoles::contains)) {
                setErrorData(response, "Only MDO Admins and Leaders can access the Bulk Transfer feature",
                        HttpStatus.FORBIDDEN);
                return response;
            }

            SBApiResponse uploadResponse = storageService.uploadFile(file,
                    serverConfig.getOrgHierarchyBulkUploadContainerName());
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                setErrorData(response, "Failed to upload file: " +
                                uploadResponse.getParams().getErrmsg(),
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            String identifier = UUID.randomUUID().toString();
            Map<String, Object> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.ROOT_ORG_ID, orgId);
            uploadedFile.put(Constants.IDENTIFIER, identifier);
            uploadedFile.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
            uploadedFile.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
            uploadedFile.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
            uploadedFile.put(Constants.STATUS, Constants.INITIATED_CAPITAL);
            uploadedFile.put(Constants.CREATED_BY, userId);

            SBApiResponse insertResponse = cassandraOperation.insertRecord(
                    Constants.DATABASE,
                    Constants.ORG_USER_BULK_TRANSFER_TABLE,
                    uploadedFile
            );

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with bulk transfer details",
                        HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(uploadedFile);
            uploadedFile.put(Constants.X_AUTH_TOKEN, userAuthToken);
            uploadedFile.put(Constants.FRAMEWORK_ID, frameworkId);
            kafkaProducer.pushWithKey(serverConfig.getOrgHierarchyUserBulkTransferTopic(),
                    uploadedFile, rootOrgId);

        } catch (Exception e) {
            log.error("Error in bulk transfer upload: ", e);
            setErrorData(response, "Failed to process bulk transfer request: " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private void setErrorData(SBApiResponse response, String errMsg, HttpStatus httpStatus) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(httpStatus);
    }

    @Override
    public SBApiResponse getBulkUploadDetailsForOrgDesignationMapping(String orgId, String rootOrgId, String userAuthToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_DESIGNATION_BULK_UPLOAD_STATUS);
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            if (StringUtils.isNotBlank(orgId)) {
                propertyMap.put(Constants.ROOT_ORG_ID, orgId);
            }
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (!validateUserOrgId(rootOrgId, userId)) {
                log.error("User is not authorized to get the fileInfo for other org: " + rootOrgId + ", request orgId " + orgId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("User is not authorized to get the fileInfo for other org");
                response.setResponseCode(HttpStatus.UNAUTHORIZED);
                return response;
            }
            List<Map<String, Object>> bulkUploadList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    Constants.ORG_USER_BULK_TRANSFER_TABLE, propertyMap, null);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.CONTENT, bulkUploadList);
            response.getResult().put(Constants.COUNT, bulkUploadList != null ? bulkUploadList.size() : 0);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to get user bulk upload request status. Error: ", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private boolean validateUserOrgId(String orgId, String userId) {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        userUtilityService.getUserDetailsFromDB(Arrays.asList(userId), Arrays.asList(Constants.USER_ID, Constants.ROOT_ORG_ID, Constants.CHANNEL), userInfoMap);
        if (org.apache.commons.collections4.MapUtils.isNotEmpty(userInfoMap)) {
            String rootOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
            String channel = userInfoMap.get(userId).get(Constants.CHANNEL);
            return (StringUtils.equalsIgnoreCase(serverConfig.getSpvChannelName(), channel) || StringUtils.equalsIgnoreCase(orgId, rootOrgId));
        }
        return false;
    }

    @Override
    public ResponseEntity<?> downloadFile(String fileName, String rootOrgId, String userAuthToken) {
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                log.error("Not able to get userId from authToken ");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
            }
            if (!validateUserOrgId(rootOrgId, userId)) {
                log.error("User is not authorized to download the file for other org");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
            }
            storageService.downloadFile(fileName, serverConfig.getOrgHierarchyBulkUploadContainerName());
            Path tmpPath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);
            ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(tmpPath));
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(tmpPath.toFile().length())
                    .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                    .body(resource);
        } catch (IOException e) {
            log.error("Failed to read the downloaded file: " + fileName + ", Exception: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            try {
                File file = new File(Constants.LOCAL_BASE_PATH + fileName);
                if (file.exists()) {
                    file.delete();
                }
            } catch (Exception e1) {
            }
        }
    }


}
