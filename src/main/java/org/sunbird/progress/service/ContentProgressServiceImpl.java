package org.sunbird.progress.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.model.SunbirdApiRequest;
import org.sunbird.common.model.SunbirdApiResp;
import org.sunbird.common.service.ContentServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.exception.InvalidDataInputException;
import org.sunbird.core.logger.CbExtLogger;
import org.sunbird.core.producer.Producer;
import org.sunbird.progress.model.ContentProgressInfo;
import org.sunbird.user.service.UserUtilityService;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;


@Service
public class ContentProgressServiceImpl implements ContentProgressService {

    @Autowired
    private CbExtServerProperties cbExtServerProperties;
    @Autowired
    Producer kafkaProducer;

    private CbExtLogger logger = new CbExtLogger(getClass().getName());

    private ObjectMapper mapper = new ObjectMapper();

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private UserUtilityService userUtilityService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ContentServiceImpl contentServiceImpl;


    /**
     * Marking the attendance for offline sessions
     *
     * @param authUserToken- It's authorization token received in request header.
     * @param requestBody    -Request body of the API which needs to be processed.
     * @return- Return the response of success/failure after processing the request.
     */
    @Override
    public SBApiResponse updateContentProgress(String authUserToken, SunbirdApiRequest requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_UPDATE_CONTENT_PROGRESS);
        Map<String, Object> contentProgressAttributes;
        Map<String, String> headersValues = new HashMap<>();
        headersValues.put("X-Authenticated-User-Token", authUserToken);
        headersValues.put("Authorization", cbExtServerProperties.getSbApiKey());
        try {
            contentProgressAttributes = new HashMap<>();
            contentProgressAttributes.put("requestBody", requestBody);
            contentProgressAttributes.put("headersValues", headersValues);
            kafkaProducer.push(cbExtServerProperties.getUpdateContentProgressKafkaTopic(), contentProgressAttributes);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception ex) {
            logger.error(ex);
            response.getParams().setErrmsg(String.format(Constants.UPDATE_CONTENT_PROGRESS_ERROR_MSG, ex.getMessage()));
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


    /**
     * This method is responsible for fetching the user content progress and user details.
     *
     * @param requestBody   -Request body of the API which needs to be processed.
     * @param authUserToken - It's authorization token received in request header.
     * @return - Return the response of success/failure after processing the request.
     */
    @Override
    public SBApiResponse getUserSessionDetailsAndCourseProgress(String authUserToken, SunbirdApiRequest requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.USER_SEARCH_CONTENT_RESULT_LIST);
        try {
            List<String> usersList = new ArrayList<>();
            ContentProgressInfo contentProgressInfo = new ContentProgressInfo();
            if (!ObjectUtils.isEmpty(requestBody.getRequest())) {
                contentProgressInfo = mapper.convertValue(requestBody.getRequest(), ContentProgressInfo.class);
                validateContentProgressInfo(contentProgressInfo);
            }
            if (contentProgressInfo.getUserId() != null && contentProgressInfo.getUserId().size() > 0) {
                usersList.addAll(contentProgressInfo.getUserId());
            } else {
                List<Map<String, Object>> enrollmentBatchLookupList = getEnrollmentBatchLookupDetails(contentProgressInfo);
                enrollmentBatchLookupList.forEach(userEnrollmentBatchDetail -> userEnrollmentBatchDetail.entrySet().stream().filter(entry -> entry.getKey().equalsIgnoreCase(Constants.USER_ID)).forEach(entry -> {
                    if (entry.getKey().equalsIgnoreCase(Constants.USER_ID)) {
                        usersList.add(entry.getValue().toString());
                    }
                }));
            }
            //final Map<String, Map<String, Object>> contentMaps = prepareProgressDetailsMap(contentProgressInfo.getContentId());
            logger.info(" Troubleshoot start of the getUserContentConsumptionDetails : 1");
            List<Map<String, Object>> userContentConsumptionList = getUserContentConsumptionDetails(contentProgressInfo, usersList);
            logger.info(" Troubleshoot End of the getUserContentConsumptionDetails : 1");
            Map<String, Map<String, Object>> userDetailsList = userUtilityService.getUserDetailsFromES(usersList, Arrays.asList(Constants.USER_FIRST_NAME, Constants.PROFILE_DETAILS_DESIGNATION, Constants.PROFILE_DETAILS_PRIMARY_EMAIL, Constants.CHANNEL, Constants.USER_ID, Constants.EMPLOYMENT_DETAILS_DEPARTMENT_NAME, Constants.PROFILE_DETAILS_PHONE, Constants.ROOT_ORG_ID));
            userContentConsumptionList.forEach(contentMap -> {
                String userId = (String) contentMap.get(Constants.USER_ID);
                String contentId = (String) contentMap.get("contentid");
               /* userDetailsList.get(userId).computeIfAbsent("progressDetails", key -> {
                    return new HashMap<>(contentMaps);
                });*/
                contentMap.remove(Constants.USER_ID);
                Map<String, Object> userMap = userDetailsList.get(userId);
                if (userMap.containsKey("progressDetails")) {
                    Map<String, Map<String, Object>> progressDetailsMap = (Map<String, Map<String, Object>>) userMap.get("progressDetails");
                    progressDetailsMap.compute(contentId, (key, existingValue) -> {
                        return contentMap;
                    });
                } else {
                    Map<String, Map<String, Object>> progressDetailsMap = new HashMap<String, Map<String, Object>>();
                    Map<String, Object> progressMap = new HashMap<String, Object>();
                    progressMap.put("contentId", contentId);
                    progressMap.put("status", contentMap.get("status"));
                    progressDetailsMap.put(contentId, progressMap);
                    userMap.put("progressDetails", progressDetailsMap);

                }
            });
            userDetailsList.forEach((userId, userInformation) -> {
                if (userInformation.containsKey("progressDetails")) {
                    Map<String, Object> progressDetails = (Map<String, Object>) userInformation.get("progressDetails");
                    userInformation.put("progressDetails", new ArrayList<>(progressDetails.values()));
                } else {
                    userInformation.put("progressDetails", new ArrayList<>());
                }
            });
            response.getResult().put(Constants.COUNT, userDetailsList.size());
            response.getResult().put(Constants.RESPONSE, userDetailsList.values());
        } catch (InvalidDataInputException exception) {
            logger.error("Exception Occurred ",exception);
            response.getParams().setErrmsg(exception.getMessage());
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setStatus(Constants.FAILED);
        } catch (Exception e) {
            logger.error("Exception Occurred ",e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
        }
        return response;
    }

    /**
     * This method returns the user_content_consumption details for the users.
     *
     * @param contentProgressInfo - Model class for the contentProgress.
     * @param usersList           - List of users to get the content consumption details.
     * @return -return a list of the user_content_consumption details based on the userIds passed.
     */
    private List<Map<String, Object>> getUserContentConsumptionDetails(ContentProgressInfo contentProgressInfo, List<String> usersList) {
        Map<String, Object> propertyMap;
        propertyMap = new HashMap<>();
        propertyMap.put(Constants.BATCH_ID, contentProgressInfo.getBatchId());
        propertyMap.put(Constants.COURSE_ID, contentProgressInfo.getCourseId());
        propertyMap.put(Constants.USER_ID, usersList);
        if (contentProgressInfo.getContentId() != null && contentProgressInfo.getContentId().size() > 0) {
            propertyMap.put(Constants.CONTENT_ID_KEY, contentProgressInfo.getContentId());
        }
        return cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.USER_CONTENT_CONSUMPTION, propertyMap, Arrays.asList(Constants.CONTENT_ID_KEY, Constants.USER_ID, Constants.STATUS));

    }


    /**
     * This method returns the enrollment_batch_lookup details for the users.
     *
     * @param contentProgressInfo - Model class for the contentProgress.
     * @return - return a list of the enrollment_batch_lookup details based on the batchId passed.
     */
    private List<Map<String, Object>> getEnrollmentBatchLookupDetails(ContentProgressInfo contentProgressInfo) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.BATCH_ID, contentProgressInfo.getBatchId());
        List<Map<String, Object>>  list = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD_COURSES,
                Constants.TABLE_ENROLMENT_BATCH_LOOKUP, propertyMap, Arrays.asList(Constants.BATCH_ID, Constants.USER_ID,Constants.ACTIVE));
        // Stream to filter and collect only non-null "endDate" maps
        return list.stream()
                .filter(item -> {
                    return item != null && item.containsKey("active") && (boolean) item.get("active") == true;
                })
                .collect(Collectors.toList());
    }

    /*private Map<String, Map<String, Object>> prepareProgressDetailsMap(List<String> contentIdList) {
        Map<String, Map<String, Object>> progressDetailsMap = new HashMap<String, Map<String, Object>>();
        for (String contentId : contentIdList) {
            Map<String, Object> progressMap = new HashMap<String, Object>();
            progressMap.put("contentId", contentId);
            progressMap.put("status", 0);
            progressDetailsMap.put(contentId, progressMap);
        }
        return progressDetailsMap;
    }*/

    private void validateContentProgressInfo(ContentProgressInfo contentProgressInfo) {
        if (StringUtils.isEmpty(contentProgressInfo.getBatchId())) {
            throw new InvalidDataInputException(Constants.CONTENT_PROGRESS_BATCH_ID_ERROR_MSG);
        }

        if (StringUtils.isEmpty(contentProgressInfo.getCourseId())) {
            throw new InvalidDataInputException(Constants.CONTENT_PROGRESS_COURSE_ID_ERROR_MSG);
        }

        if (contentProgressInfo.getContentId().isEmpty()) {
            throw new InvalidDataInputException(Constants.CONTENT_PROGRESS_CONTENT_ID_ERROR_MSG);
        }
    }

    @Override
    public SBApiResponse updateContentAttendance( SunbirdApiRequest requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_UPDATE_CONTENT_ATTENDANCE);
        Map<String, Object> contentProgressAttributes;
        try {
            Map<String, Object> req = (Map<String, Object>) requestBody.getRequest();
            String externalCourseID = (String) req.get(Constants.EXTERNAL_CONTENT_ID);
            String courseId = fetchIgotContentIdByExternalId(externalCourseID);
            if (StringUtils.isEmpty(courseId)) {
                logger.warn("No courseId found for given externalContentId: " + externalCourseID);
                response.getParams().setErrmsg(Constants.COURSE_ID_NOT_AVAILABLE_ERROR_MSG);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                return response;
            }
            String batchId = "";
            String userId = (String) req.get(Constants.USER_ID);
            Optional<String> result = extractBatchId(userId, courseId);
            if (result.isPresent()) {
                 batchId = result.get();
            } else {
                logger.info("No batchId found for given userId: " + userId + ", and courseId: " + courseId);
            }
            String sessionId = extractSessionIdFromProgram(courseId, batchId);
            if (ObjectUtils.isEmpty(sessionId)) {
                logger.warn("No sessionId  found for given programId: " + courseId + ", and batchId: " + batchId);
                response.getParams().setErrmsg(Constants.ACTIVE_SESSION_ID_NOT_AVAILABLE_ERROR_MSG);
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.NOT_FOUND);
                return response;
            }

            Map<String, Object> contentProgress = new HashMap<>();
            contentProgress.put(Constants.CONTENT_ID_KEY, sessionId);
            contentProgress.put(Constants.BATCH_ID, batchId);
            contentProgress.put(Constants.COURSE_ID, courseId);
            contentProgress.put(Constants.STATUS, 2);
            contentProgress.put(Constants.COMPLETION_PERCENTAGE, 100);
            contentProgress.put("lastAccessTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSSZ").format(new Date()));

            Map<String, Object> requestMap = new HashMap<>();
            requestMap.put(Constants.USER_ID, userId);
            requestMap.put("contents", Collections.singletonList(contentProgress));

            Map<String, Object> requestWrapper = new HashMap<>();
            requestWrapper.put("request", Collections.singletonList(requestMap));

            contentProgressAttributes = new HashMap<>();
            contentProgressAttributes.put("requestBody", requestWrapper);
            kafkaProducer.push(cbExtServerProperties.getUpdateContentProgressKafkaTopic(), contentProgressAttributes);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResult(contentProgressAttributes);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception ex) {
            logger.error("Exception occurred while updating content attendance: ", ex);
            response.getParams().setErrmsg(String.format(Constants.UPDATE_CONTENT_ATTENDANCE_PROGRESS_ERROR_MSG, ex.getMessage()));
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private String extractSessionIdFromProgram(String courseId, String batchId) {
        try {
            SunbirdApiResp contentHeirarchyResult = contentServiceImpl.getHeirarchyResponse(courseId);
            if (contentHeirarchyResult == null ||
                    !"successful".equalsIgnoreCase(contentHeirarchyResult.getParams().getStatus()) ||
                    contentHeirarchyResult.getResult() == null || contentHeirarchyResult.getResult().getContent() == null) {
                logger.warn("Invalid content hierarchy response for programId: " + courseId);
                return null;
            }
            Map<String, Object> contentMap = mapper.convertValue(
                    contentHeirarchyResult.getResult().getContent(),
                    new TypeReference<Map<String, Object>>() {}
            );
            List<Map<String, Object>> batches = (List<Map<String, Object>>) contentMap.get("batches");

            if (batches == null || batches.isEmpty()) {
                logger.warn("No batches found for programId:" + courseId);
                return null;
            }
            // Iterate through batches to find the matching batchId and extract sessionId
            for (Map<String, Object> batch : batches) {
                String currentBatchId = (String) batch.get("batchId");
                if (batchId.equals(currentBatchId)) {
                    Map<String, Object> batchAttributes = (Map<String, Object>) batch.get("batchAttributes");
                    if (batchAttributes != null) {
                        List<Map<String, Object>> sessionDetails = (List<Map<String, Object>>) batchAttributes.get("sessionDetails_v2");
                        if (sessionDetails != null && !sessionDetails.isEmpty()) {
                            Map<String, Object> session = sessionDetails.get(0);
                            return (String) session.get("sessionId");
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error extracting sessionId for programId: " + courseId + " and batchId: " + batchId, e);
        }
        return null;
    }

    public Optional<String> extractBatchId(
            String userId,
            String courseId
    ) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.PUBLIC_USER_ID, userId);
        propertyMap.put(Constants.COURSE_ID_COLUMN, courseId);

        List<String> fieldsToFetch = Arrays.asList("batchid", "active");
        List<Map<String, Object>> enrolments = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                "sunbird_courses",
                "user_enrolments",
                propertyMap,
                fieldsToFetch
        );
        if (CollectionUtils.isNotEmpty(enrolments)) {
            for (Map<String, Object> enrolment : enrolments) {
                Boolean active = (Boolean) enrolment.get("active");
                String batchId = (String) enrolment.get("batchId");
                    if (Boolean.TRUE.equals(active)) {
                        return Optional.ofNullable(batchId);
                    } else {
                        logger.info(String.format("User [%s] has an inactive enrollment for course [%s].", userId, courseId));
                    }
                }
            }
         else {
            logger.info("No batchId  found from enrollment for given userId and courseId.");
        }
        return Optional.empty();
    }

    public String fetchIgotContentIdByExternalId(String externalContentId) {
        Map<String, Object> properties = new HashMap<>();
        properties.put("external_content_id", externalContentId);
        List<String> fieldsToFetch = Arrays.asList("igot_content_id", "content_partner_name");

        List<Map<String, Object>> records = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD,
                Constants.KEYSPACE_EXTERNAL_INTEGRATION_COURSES,
                properties,
                fieldsToFetch
        );
        if (CollectionUtils.isNotEmpty(records)) {
            return (String) records.get(0).get("igot_content_id");
        } else {
            logger.warn("No record found in external_course_mapping for external_content_id: " + externalContentId);
            return null;
        }
    }

}
