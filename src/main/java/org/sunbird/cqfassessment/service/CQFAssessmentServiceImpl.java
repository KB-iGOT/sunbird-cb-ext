package org.sunbird.cqfassessment.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.assessment.repo.AssessmentRepository;
import org.sunbird.assessment.service.AssessmentUtilServiceV2;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Service implementation for managing CQF Assessments.
 */

@Service
public class CQFAssessmentServiceImpl implements CQFAssessmentService {
    private final Logger logger = LoggerFactory.getLogger(CQFAssessmentServiceImpl.class);

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    AssessmentUtilServiceV2 assessUtilServ;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    AssessmentRepository assessmentRepository;
    /**
     * Creates a entry for new CQF Assessment.
     *
     * @param authToken   the authentication token for the request
     * @param requestBody the request body containing the assessmentId and the status
     * @return the API response containing the created assessment details
     */
    @Override
    public SBApiResponse createCQFAssessment(String authToken, Map<String, Object> requestBody) {
        logger.info("CQFAssessmentServiceImpl::createCQFAssessment.. started");
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CQF_API_CREATE_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(outgoingResponse, HttpStatus.BAD_REQUEST);
            return outgoingResponse;
        }
        String errMsg = validateRequest(requestBody, outgoingResponse);
        if (StringUtils.isNotBlank(errMsg)) {
            return outgoingResponse;
        }
        checkActiveCqfAssessments(requestBody);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ASSESSMENT_ID_KEY, requestBody.get(Constants.ASSESSMENT_ID_KEY));
        request.put(Constants.ACTIVE_STATUS, requestBody.get(Constants.ACTIVE_STATUS));
        return cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, request);
    }

    /**
     * Updates an existing CQF Assessment.
     *
     * @param requestBody             the request body containing the updated assessment status
     * @param authToken               the authentication token for the request
     * @param cqfAssessmentIdentifier the identifier of the assessment to update
     * @return the API response containing the updated assessment details
     */
    @Override
    public SBApiResponse updateCQFAssessment(Map<String, Object> requestBody, String authToken, String cqfAssessmentIdentifier) {
        logger.info("CQFAssessmentServiceImpl::updateCQFAssessment.. started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_UPDATE_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(response, HttpStatus.BAD_REQUEST);
            return response;
        }
        String errMsg = validateRequest(requestBody, response);
        if (StringUtils.isNotBlank(errMsg)) {
            return response;
        }
        checkActiveCqfAssessments(requestBody);
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.ACTIVE_STATUS, requestBody.get(Constants.ACTIVE_STATUS));
        Map<String, Object> compositeKeyMap = new HashMap<>();
        compositeKeyMap.put(Constants.ASSESSMENT_ID_KEY, cqfAssessmentIdentifier);
        Map<String, Object> resp = cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, request, compositeKeyMap);
        response.getResult().put(Constants.CQF_ASSESSMENT_DATA, resp);
        return response;
    }

    /**
     * Retrieves a CQF Assessment by its identifier.
     *
     * @param authToken               the authentication token for the request
     * @param cqfAssessmentIdentifier the identifier of the assessment to retrieve
     * @return the API response containing the assessment details
     */
    @Override
    public SBApiResponse getCQFAssessment(String authToken, String cqfAssessmentIdentifier) {
        logger.info("CQFAssessmentServiceImpl::getCQFAssessment... Started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_READ_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(response, HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ASSESSMENT_ID_KEY, cqfAssessmentIdentifier);
        Map<String, Object> cqfAssessmentDataList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, propertyMap, Arrays.asList(Constants.ASSESSMENT_ID_KEY, Constants.ACTIVE_STATUS), Constants.ASSESSMENT_ID_KEY);
        response.getResult().put(Constants.CQF_ASSESSMENT_DATA, cqfAssessmentDataList);
        return response;
    }


    /**
     * Lists all CQF Assessments.
     *
     * @param authToken the authentication token for the request
     * @return the API response containing the list of assessments
     */
    @Override
    public SBApiResponse listCQFAssessments(String authToken) {
        logger.info("CQFAssessmentServiceImpl::listCQFAssessments... Started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_LIST_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(response, HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        List<Map<String, Object>> cqfAssessmentDataList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, propertyMap, new ArrayList<>());
        response.getResult().put(Constants.CQF_ASSESSMENT_DATA, cqfAssessmentDataList);
        return response;
    }

    /**
     * Updates the error details in the API response.
     *
     * @param response The API response object.
     * @param responseCode The HTTP status code.
     */
    private void updateErrorDetails(SBApiResponse response, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
        response.setResponseCode(responseCode);
    }



    /**
     * Validates the request and updates the API response accordingly.
     *
     * @param request The request object.
     * @param response The API response object.
     * @return An error message if the request is invalid, otherwise an empty string.
     */
    private String validateRequest(Map<String, Object> request, SBApiResponse response) {
        if (MapUtils.isEmpty(request)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("RequestBody is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Request Body is missing";
        }
        else if (StringUtils.isBlank((String) request.get(Constants.ASSESSMENT_ID_KEY))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Assessment Id is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Assessment Id is missing";
        } else if (StringUtils.isBlank((String) request.get(Constants.ACTIVE_STATUS))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Active status is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Active status is missing";
        }
        return "";
    }

    /**
     * Checks for active CQF assessments and updates their status to inactive if necessary.
     *
     * @param requestBody The request body containing the active status.
     */
    public void checkActiveCqfAssessments(Map<String, Object> requestBody) {
        String activeStatus = (String) requestBody.get(Constants.ACTIVE_STATUS);
        if ("active".equals(activeStatus)) {
            Map<String, Object> propertyMap = new HashMap<>();
            List<Map<String, Object>> recordsToUpdate = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, propertyMap, new ArrayList<>());
            if (recordsToUpdate.stream()
                    .anyMatch(assessmentRecord -> assessmentRecord.get(Constants.ACTIVE_STATUS).equals("active"))) {
                recordsToUpdate.forEach(assessmentRecord -> {
                    Map<String, Object> request = new HashMap<>();
                    request.put(Constants.ACTIVE_STATUS, "inactive");
                    Map<String, Object> compositeKeyMap = new HashMap<>();
                    compositeKeyMap.put(Constants.ASSESSMENT_ID_KEY, assessmentRecord.get(Constants.ASSESSMENT_ID_KEY));
                    cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.CQF_ASSESSMENT_TRACKING, request, compositeKeyMap);
                });
            }
        }
    }

    /**
     * Reads an assessment based on the provided assessment identifier and token.
     *
     * @param assessmentIdentifier The identifier of the assessment to be read.
     * @param token                The access token used to authenticate the user.
     * @param editMode             A flag indicating whether the assessment is being read in edit mode.
     * @return The SBApiResponse containing the assessment details.
     */
    @Override
    public SBApiResponse readAssessment(String assessmentIdentifier, String token, boolean editMode) {
        logger.info("AssessmentServicev5Impl::readAssessment... Started");
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_READ_ASSESSMENT);
        String errMsg = "";
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(token);
            if (StringUtils.isBlank(userId)) {
                return handleUserIdDoesNotExist(response);
            }
            logger.info(String.format("ReadAssessment... UserId: %s, AssessmentIdentifier: %s", userId, assessmentIdentifier));
            Map<String, Object> assessmentAllDetail = fetchAssessmentDetails(assessmentIdentifier, token, editMode);
            if (MapUtils.isEmpty(assessmentAllDetail)) {
                return handleAssessmentHierarchyReadFailed(response);
            }
            if (isPracticeAssessmentOrEditMode(assessmentAllDetail, editMode)) {
                return handlePracticeAssessment(response, assessmentAllDetail);
            }
            return handleUserSubmittedAssessment(userId, assessmentIdentifier, response, assessmentAllDetail);
        } catch (Exception e) {
            errMsg = String.format("Error while reading assessment. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
        }
        return response;
    }

    /**
     * Handles the case where the user ID is not found in the access token.
     *
     * @param response The SBApiResponse to be updated with error details.
     * @return The updated SBApiResponse with error details.
     */
    private SBApiResponse handleUserIdDoesNotExist(SBApiResponse response) {
        updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST);
        return response;
    }


    /**
     * Fetches the assessment details based on the provided assessment identifier, token, and edit mode.
     *
     * @param assessmentIdentifier The identifier of the assessment to be fetched.
     * @param token                The access token used to authenticate the user.
     * @param editMode             A flag indicating whether the assessment is being fetched in edit mode.
     * @return A map containing the assessment details.
     */
    private Map<String, Object> fetchAssessmentDetails(String assessmentIdentifier, String token, boolean editMode) {
        // If edit mode is enabled, fetch the assessment hierarchy from the assessment service
        // This ensures that the latest assessment data is retrieved from the service
        return editMode
                ? assessUtilServ.fetchHierarchyFromAssessServc(assessmentIdentifier, token)
                // If edit mode is disabled, read the assessment hierarchy from the cache
                // This optimizes performance by reducing the number of service calls
                : assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdentifier, editMode, token);
    }

    /**
     * Handles the case where the assessment hierarchy read fails.
     *
     * @param response The SBApiResponse to be updated with error details.
     * @return The updated SBApiResponse with error details.
     */
    private SBApiResponse handleAssessmentHierarchyReadFailed(SBApiResponse response) {
        // Update the response with error details indicating that the assessment hierarchy read failed
        updateErrorDetails(response, Constants.ASSESSMENT_HIERARCHY_READ_FAILED);
        return response;
    }

    /**
     * Checks if the assessment is a practice assessment or if it's being read in edit mode.
     *
     * @param assessmentAllDetail The map containing the assessment details.
     * @param editMode            A flag indicating whether the assessment is being read in edit mode.
     * @return True if the assessment is a practice assessment or if it's being read in edit mode, false otherwise.
     */
    private boolean isPracticeAssessmentOrEditMode(Map<String, Object> assessmentAllDetail, boolean editMode) {
        return Constants.PRACTICE_QUESTION_SET.equalsIgnoreCase((String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY)) || editMode;
    }


    /**
     * Handles a practice assessment by adding the question set data to the response.
     *
     * @param response            The SBApiResponse to be updated with the question set data.
     * @param assessmentAllDetail The map containing the assessment details.
     * @return The updated SBApiResponse with the question set data.
     */
    private SBApiResponse handlePracticeAssessment(SBApiResponse response, Map<String, Object> assessmentAllDetail) {
        response.getResult().put(Constants.QUESTION_SET, readAssessmentLevelData(assessmentAllDetail));
        return response;
    }

    /**
     * Handles a user submitted assessment by processing it based on whether it's the first time the user is taking the assessment or not.
     *
     * @param userId               The ID of the user taking the assessment.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param response             The SBApiResponse to be updated with the assessment result.
     * @param assessmentAllDetail  The map containing the assessment details.
     * @return The updated SBApiResponse with the assessment result.
     */
    private SBApiResponse handleUserSubmittedAssessment(String userId, String assessmentIdentifier, SBApiResponse response, Map<String, Object> assessmentAllDetail) {
        List<Map<String, Object>> existingDataList = assessUtilServ.readUserSubmittedAssessmentRecords(userId, assessmentIdentifier);
        Timestamp assessmentStartTime = new Timestamp(new Date().getTime());
        return processAssessment(userId, assessmentIdentifier, assessmentAllDetail, assessmentStartTime, response, existingDataList);
    }


    /**
     * Processes an assessment based on whether it's the first time the user is taking the assessment or not.
     *
     * @param userId               The ID of the user taking the assessment.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param assessmentAllDetail  The map containing the assessment details.
     * @param assessmentStartTime  The start time of the assessment.
     * @param response             The SBApiResponse to be updated with the assessment result.
     * @param existingDataList     The list of existing data for the assessment.
     * @return The updated SBApiResponse with the assessment result.
     */
    public SBApiResponse processAssessment(String userId, String assessmentIdentifier, Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, SBApiResponse response, List<Map<String, Object>> existingDataList) {
        if (existingDataList.isEmpty()) {
            return handleFirstTimeAssessment(userId, assessmentIdentifier, assessmentAllDetail, assessmentStartTime, response);
        } else {
            return handleExistingAssessment(userId, assessmentIdentifier, assessmentAllDetail, assessmentStartTime, response, existingDataList);
        }
    }

    /**
     * Handles a first time assessment by validating the assessment duration, calculating the assessment end time,
     * preparing the assessment data, and updating the assessment data to the database.
     *
     * @param userId               The ID of the user taking the assessment.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param assessmentAllDetail  The map containing the assessment details.
     * @param assessmentStartTime  The start time of the assessment.
     * @param response             The SBApiResponse to be updated with the assessment result.
     * @return The updated SBApiResponse with the assessment result.
     */
    private SBApiResponse handleFirstTimeAssessment(String userId, String assessmentIdentifier, Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, SBApiResponse response) {
        logger.info("Assessment read first time for user.");
        if (!isValidAssessmentDuration(assessmentAllDetail)) {
            updateErrorDetails(response, Constants.ASSESSMENT_INVALID);
            return response;
        }
        int expectedDuration = (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION);
        Timestamp assessmentEndTime = calculateAssessmentSubmitTime(expectedDuration, assessmentStartTime, 0);
        Map<String, Object> assessmentData = prepareAssessmentData(assessmentAllDetail, assessmentStartTime, assessmentEndTime);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);
        Map<String, Object> questionSetMap = objectMapper.convertValue(response.getResult().get(Constants.QUESTION_SET), new TypeReference<Map<String, Object>>() {
        });
        if (Boolean.FALSE.equals(updateAssessmentDataToDB(userId, assessmentIdentifier, assessmentStartTime, assessmentEndTime, questionSetMap))) {
            updateErrorDetails(response, Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED);
        }
        return response;
    }


    /**
     * Checks if the assessment duration is valid by verifying that the expected duration is not null.
     *
     * @param assessmentAllDetail The map containing the assessment details.
     * @return True if the expected duration is not null, false otherwise.
     */
    private boolean isValidAssessmentDuration(Map<String, Object> assessmentAllDetail) {
        return assessmentAllDetail.get(Constants.EXPECTED_DURATION) != null;
    }

    private Timestamp calculateAssessmentSubmitTime(int expectedDuration, Timestamp assessmentStartTime,
                                                    int bufferTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(assessmentStartTime.getTime());
        if (bufferTime > 0) {
            cal.add(Calendar.SECOND,
                    expectedDuration + Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration()));
        } else {
            cal.add(Calendar.SECOND, expectedDuration);
        }
        return new Timestamp(cal.getTime().getTime());
    }


    /**
     * Prepares the assessment data by reading the assessment level data and adding the start and end times.
     *
     * @param assessmentAllDetail The map containing the assessment details.
     * @param assessmentStartTime The start time of the assessment.
     * @param assessmentEndTime   The end time of the assessment.
     * @return The prepared assessment data.
     */
    private Map<String, Object> prepareAssessmentData(Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, Timestamp assessmentEndTime) {
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        assessmentData.put(Constants.START_TIME, assessmentStartTime.getTime());
        assessmentData.put(Constants.END_TIME, assessmentEndTime.getTime());
        return assessmentData;
    }

    /**
     * Updates the assessment data to the database.
     *
     * @param userId               The ID of the user.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param assessmentStartTime  The start time of the assessment.
     * @param assessmentEndTime    The end time of the assessment.
     * @param questionSetMap       The map of questions.
     * @return True if the update is successful, false otherwise.
     */
    private Boolean updateAssessmentDataToDB(String userId, String assessmentIdentifier, Timestamp assessmentStartTime, Timestamp assessmentEndTime, Map<String, Object> questionSetMap) {
        return assessmentRepository.addUserCQFAssesmentDataToDB(userId,
                assessmentIdentifier, assessmentStartTime, assessmentEndTime,
                questionSetMap,
                Constants.NOT_SUBMITTED);

    }

    /**
     * Handles the case where an assessment already exists for the given user.
     *
     * @param userId               The ID of the user.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param assessmentAllDetail  The details of the assessment.
     * @param assessmentStartTime  The start time of the assessment.
     * @param response             The API response object.
     * @param existingDataList     The list of existing assessment data.
     * @return The API response object.
     */
    private SBApiResponse handleExistingAssessment(String userId, String assessmentIdentifier, Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, SBApiResponse response, List<Map<String, Object>> existingDataList) {
        logger.info("Assessment read... user has details... ");
        Date existingAssessmentEndTime = (Date) existingDataList.get(0).get(Constants.END_TIME);
        Timestamp existingAssessmentEndTimeTimestamp = new Timestamp(existingAssessmentEndTime.getTime());
        String status = (String) existingDataList.get(0).get(Constants.STATUS);
        if (isAssessmentStillOngoing(assessmentStartTime, existingAssessmentEndTimeTimestamp, status)) {
            return handleOngoingAssessment(existingDataList, assessmentStartTime, existingAssessmentEndTimeTimestamp, response);
        } else if (shouldReattemptOrStartNewAssessment(assessmentStartTime, existingAssessmentEndTimeTimestamp, status)) {
            return handleAssessmentRetryOrExpired(userId, assessmentIdentifier, assessmentAllDetail, response);
        }
        return null;
    }

    /**
     * Checks if the assessment is still ongoing based on the start time, end time, and status.
     *
     * @param assessmentStartTime                The start time of the assessment.
     * @param existingAssessmentEndTimeTimestamp The end time of the existing assessment.
     * @param status                             The status of the assessment.
     * @return True if the assessment is still ongoing, false otherwise.
     */
    private boolean isAssessmentStillOngoing(Timestamp assessmentStartTime, Timestamp existingAssessmentEndTimeTimestamp, String status) {
        return assessmentStartTime.compareTo(existingAssessmentEndTimeTimestamp) < 0
                && Constants.NOT_SUBMITTED.equalsIgnoreCase(status);
    }

    /**
     * Handles the case where the assessment is still ongoing.
     *
     * @param existingDataList                   The list of existing assessment data.
     * @param assessmentStartTime                The start time of the assessment.
     * @param existingAssessmentEndTimeTimestamp The end time of the existing assessment.
     * @param response                           The API response object.
     * @return The API response object with the updated question set.
     */
    private SBApiResponse handleOngoingAssessment(List<Map<String, Object>> existingDataList, Timestamp assessmentStartTime, Timestamp existingAssessmentEndTimeTimestamp, SBApiResponse response) {
        String questionSetFromAssessmentString = (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
        Map<String, Object> questionSetFromAssessment = new Gson().fromJson(
                questionSetFromAssessmentString, new TypeToken<HashMap<String, Object>>() {
                }.getType());
        questionSetFromAssessment.put(Constants.START_TIME, assessmentStartTime.getTime());
        questionSetFromAssessment.put(Constants.END_TIME, existingAssessmentEndTimeTimestamp.getTime());
        response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
        return response;
    }

    /**
     * Checks if the assessment should be reattempted or started anew based on the start time, end time, and status.
     *
     * @param assessmentStartTime                The start time of the assessment.
     * @param existingAssessmentEndTimeTimestamp The end time of the existing assessment.
     * @param status                             The status of the assessment.
     * @return True if the assessment should be reattempted or started anew, false otherwise.
     */
    private boolean shouldReattemptOrStartNewAssessment(Timestamp assessmentStartTime, Timestamp existingAssessmentEndTimeTimestamp, String status) {
        return (assessmentStartTime.compareTo(existingAssessmentEndTimeTimestamp) < 0
                && Constants.SUBMITTED.equalsIgnoreCase(status))
                || assessmentStartTime.compareTo(existingAssessmentEndTimeTimestamp) > 0;
    }

    /**
     * Handles the case where the assessment should be reattempted or started anew.
     *
     * @param userId               The ID of the user.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param assessmentAllDetail  The details of the assessment.
     * @param response             The API response object.
     * @return The API response object with the updated assessment data.
     */
    private SBApiResponse handleAssessmentRetryOrExpired(String userId, String assessmentIdentifier, Map<String, Object> assessmentAllDetail, SBApiResponse response) {
        logger.info("Incase the assessment is submitted before the end time, or the endtime has exceeded, read assessment freshly ");

        if (isMaxRetakeAttemptsExceeded(userId, assessmentIdentifier, assessmentAllDetail)) {
            updateErrorDetails(response, Constants.ASSESSMENT_RETRY_ATTEMPTS_CROSSED);
            return response;
        }
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        Timestamp assessmentStartTime = new Timestamp(new Date().getTime());
        Timestamp assessmentEndTime = calculateAssessmentSubmitTime(
                (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION),
                assessmentStartTime, 0);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);

        if (Boolean.FALSE.equals(updateAssessmentDataToDB(userId, assessmentIdentifier, assessmentStartTime, assessmentEndTime, assessmentData))) {
            updateErrorDetails(response, Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED);
        }

        return response;
    }

    /**
     * Checks if the maximum retake attempts have been exceeded.
     *
     * @param userId               The ID of the user.
     * @param assessmentIdentifier The identifier of the assessment.
     * @param assessmentAllDetail  The details of the assessment.
     * @return True if the maximum retake attempts have been exceeded, false otherwise.
     */
    private boolean isMaxRetakeAttemptsExceeded(String userId, String assessmentIdentifier, Map<String, Object> assessmentAllDetail) {
        if (assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) != null) {
            int retakeAttemptsAllowed = (int) assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) + 1;
            int retakeAttemptsConsumed = calculateAssessmentRetakeCount(userId, assessmentIdentifier);
            return retakeAttemptsConsumed >= retakeAttemptsAllowed;
        }
        return false;
    }


    /**
     * Calculates the number of retake attempts made by a user for a specific assessment.
     *
     * @param userId       The ID of the user.
     * @param assessmentId The ID of the assessment.
     * @return The number of retake attempts made by the user for the assessment.
     */
    private int calculateAssessmentRetakeCount(String userId, String assessmentId) {
        List<Map<String, Object>> userAssessmentDataList = assessUtilServ.readUserSubmittedAssessmentRecords(userId,
                assessmentId);
        return (int) userAssessmentDataList.stream()
                .filter(userData -> userData.containsKey(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY)
                        && null != userData.get(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY))
                .count();
    }


    /**
     * Updates the error details in the API response.
     *
     * @param response The API response object.
     * @param errMsg   The error message to be set in the response.
     */

    private void updateErrorDetails(SBApiResponse response, String errMsg) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private Map<String, Object> readAssessmentLevelData(Map<String, Object> assessmentAllDetail) {
        List<String> assessmentParams = serverProperties.getAssessmentLevelParams();
        Map<String, Object> assessmentFilteredDetail = new HashMap<>();
        for (String assessmentParam : assessmentParams) {
            if ((assessmentAllDetail.containsKey(assessmentParam))) {
                assessmentFilteredDetail.put(assessmentParam, assessmentAllDetail.get(assessmentParam));
            }
        }
        readSectionLevelParams(assessmentAllDetail, assessmentFilteredDetail);
        return assessmentFilteredDetail;
    }

    private void readSectionLevelParams(Map<String, Object> assessmentAllDetail,
                                        Map<String, Object> assessmentFilteredDetail) {
        List<Map<String, Object>> sectionResponse = new ArrayList<>();
        List<String> sectionIdList = new ArrayList<>();
        List<String> sectionParams = serverProperties.getAssessmentSectionParams();
        List<Map<String, Object>> sections = objectMapper.convertValue(assessmentAllDetail.get(Constants.CHILDREN), new TypeReference<List<Map<String, Object>>>() {
        });
        for (Map<String, Object> section : sections) {
            sectionIdList.add((String) section.get(Constants.IDENTIFIER));
            Map<String, Object> newSection = new HashMap<>();
            for (String sectionParam : sectionParams) {
                if (section.containsKey(sectionParam)) {
                    newSection.put(sectionParam, section.get(sectionParam));
                }
            }
            List<Map<String, Object>> questions = objectMapper.convertValue(section.get(Constants.CHILDREN), new TypeReference<List<Map<String, Object>>>() {
            });
            int maxQuestions = (int) section.getOrDefault(Constants.MAX_QUESTIONS, questions.size());
            List<String> childNodeList = questions.stream()
                    .map(question -> (String) question.get(Constants.IDENTIFIER))
                    .limit(maxQuestions)
                    .collect(Collectors.toList());
            newSection.put(Constants.CHILD_NODES, childNodeList);
            sectionResponse.add(newSection);
        }
        assessmentFilteredDetail.put(Constants.CHILDREN, sectionResponse);
        assessmentFilteredDetail.put(Constants.CHILD_NODES, sectionIdList);
    }
}