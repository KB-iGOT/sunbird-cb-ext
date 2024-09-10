package org.sunbird.cqfassessment.service;

import com.beust.jcommander.internal.Lists;
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
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.assessment.repo.AssessmentRepository;
import org.sunbird.assessment.service.AssessmentUtilServiceV2;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.cqfassessment.model.CQFAssessmentModel;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 * @author mahesh.vakkund
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
     * Reads an assessment based on the provided assessment identifier, content ID, and version key.
     *
     * @param assessmentIdentifier The unique identifier of the assessment.
     * @param token                The access token for authentication.
     * @param editMode             A boolean indicating whether the assessment is being read in edit mode.
     * @param contentId            The ID of the content being assessed.
     * @param versionKey           The version key of the assessment.
     * @return An SBApiResponse containing the assessment details or error information.
     */
    @Override
    public SBApiResponse readAssessment(String assessmentIdentifier, String token, boolean editMode, String contentId, String versionKey) {
        logger.info("CQFAssessmentServiceImpl:readAssessment... Started");
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
            CQFAssessmentModel cqfAssessmentModel = new CQFAssessmentModel(userId, assessmentIdentifier, contentId, versionKey);
            return handleUserSubmittedAssessment(response, assessmentAllDetail, cqfAssessmentModel);
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
     * Fetches the assessment details based on the provided assessment identifier.
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
     * Handles the user-submitted assessment by reading existing records, processing the assessment, and returning the response.
     *
     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @return The SBApiResponse object containing the assessment results.
     */
    private SBApiResponse handleUserSubmittedAssessment(SBApiResponse response, Map<String, Object> assessmentAllDetail, CQFAssessmentModel cqfAssessmentModel) {
        List<Map<String, Object>> existingDataList = readUserSubmittedAssessmentRecords(cqfAssessmentModel);
        Timestamp assessmentStartTime = new Timestamp(new Date().getTime());
        return processAssessment(assessmentAllDetail, assessmentStartTime, response, existingDataList, cqfAssessmentModel);
    }


    /**
     * Processes the assessment based on whether it's a first-time assessment or an existing one.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @param assessmentStartTime The timestamp marking the start of the assessment.
     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param existingDataList    A list of maps containing existing assessment data.
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @return The SBApiResponse object containing the assessment results.
     */
    public SBApiResponse processAssessment(Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, SBApiResponse response, List<Map<String, Object>> existingDataList, CQFAssessmentModel cqfAssessmentModel) {
        if (existingDataList.isEmpty()) {
            return handleFirstTimeAssessment(assessmentAllDetail, assessmentStartTime, response, cqfAssessmentModel);
        } else {
            return handleExistingAssessment(assessmentAllDetail, assessmentStartTime, response, existingDataList, cqfAssessmentModel);
        }
    }

    /**
     * Handles the first-time assessment by preparing the assessment data and updating it to the database.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @param assessmentStartTime The timestamp marking the start of the assessment.
     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @return The SBApiResponse object containing the assessment results.
     */
    private SBApiResponse handleFirstTimeAssessment(Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, SBApiResponse response, CQFAssessmentModel cqfAssessmentModel) {
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
        if (Boolean.FALSE.equals(updateAssessmentDataToDB(cqfAssessmentModel, assessmentStartTime, assessmentEndTime, questionSetMap))) {
            updateErrorDetails(response, Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED);
        }
        return response;
    }


    /**
     * Checks if the assessment duration is valid by verifying that the expected duration is a positive integer.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @return True if the assessment duration is valid, false otherwise.
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
     * Updates the assessment data to the database by adding the user's CQF assessment data.
     *
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @param assessmentStartTime The timestamp marking the start of the assessment.
     * @param assessmentEndTime   The timestamp marking the end of the assessment.
     * @param questionSetMap      A map containing the question set data.
     * @return True if the assessment data was updated successfully, false otherwise.
     */
    private Boolean updateAssessmentDataToDB(CQFAssessmentModel cqfAssessmentModel, Timestamp assessmentStartTime, Timestamp assessmentEndTime, Map<String, Object> questionSetMap) {
        return assessmentRepository.addUserCQFAssesmentDataToDB(cqfAssessmentModel, assessmentStartTime, assessmentEndTime,
                questionSetMap,
                Constants.NOT_SUBMITTED);

    }

    /**
     * Handles an existing assessment by determining whether it is still ongoing, can be reattempted, or has expired.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @param assessmentStartTime The timestamp marking the start of the assessment.
     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param existingDataList    A list of maps containing the existing assessment data.
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @return The SBApiResponse object containing the assessment results, or null if the assessment is not handled.
     */
    private SBApiResponse handleExistingAssessment(Map<String, Object> assessmentAllDetail, Timestamp assessmentStartTime, SBApiResponse response, List<Map<String, Object>> existingDataList, CQFAssessmentModel cqfAssessmentModel) {
        logger.info("Assessment read... user has details... ");
        Date existingAssessmentEndTime = (Date) existingDataList.get(0).get(Constants.END_TIME);
        Timestamp existingAssessmentEndTimeTimestamp = new Timestamp(existingAssessmentEndTime.getTime());
        String status = (String) existingDataList.get(0).get(Constants.STATUS);
        if (isAssessmentStillOngoing(assessmentStartTime, existingAssessmentEndTimeTimestamp, status)) {
            return handleOngoingAssessment(existingDataList, assessmentStartTime, existingAssessmentEndTimeTimestamp, response);
        } else if (shouldReattemptOrStartNewAssessment(assessmentStartTime, existingAssessmentEndTimeTimestamp, status)) {
            return handleAssessmentRetryOrExpired(assessmentAllDetail, response, cqfAssessmentModel);
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


    private SBApiResponse handleAssessmentRetryOrExpired(Map<String, Object> assessmentAllDetail, SBApiResponse response, CQFAssessmentModel cqfAssessmentModel) {
        logger.info("Incase the assessment is submitted before the end time, or the endtime has exceeded, read assessment freshly ");

        if (isMaxRetakeAttemptsExceeded(cqfAssessmentModel, assessmentAllDetail)) {
            updateErrorDetails(response, Constants.ASSESSMENT_RETRY_ATTEMPTS_CROSSED);
            return response;
        }
        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        Timestamp assessmentStartTime = new Timestamp(new Date().getTime());
        Timestamp assessmentEndTime = calculateAssessmentSubmitTime(
                (Integer) assessmentAllDetail.get(Constants.EXPECTED_DURATION),
                assessmentStartTime, 0);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);

        if (Boolean.FALSE.equals(updateAssessmentDataToDB(cqfAssessmentModel, assessmentStartTime, assessmentEndTime, assessmentData))) {
            updateErrorDetails(response, Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED);
        }

        return response;
    }

    /**
     * Checks if the maximum number of retake attempts for the assessment has been exceeded.
     *
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @return True if the maximum number of retake attempts has been exceeded, false otherwise.
     */
    private boolean isMaxRetakeAttemptsExceeded(CQFAssessmentModel cqfAssessmentModel, Map<String, Object> assessmentAllDetail) {
        if (assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) != null) {
            int retakeAttemptsAllowed = (int) assessmentAllDetail.get(Constants.MAX_ASSESSMENT_RETAKE_ATTEMPTS) + 1;
            int retakeAttemptsConsumed = calculateAssessmentRetakeCount(cqfAssessmentModel.getUserId(), cqfAssessmentModel.getAssessmentIdentifier());
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

    /**
     * Reads the user's submitted assessment records for a given CQF assessment model.
     *
     * @param cqfAssessmentModel A CQFAssessmentModel object representing the assessment.
     * @return A list of maps containing the user's submitted assessment records.
     */
    public List<Map<String, Object>> readUserSubmittedAssessmentRecords(CQFAssessmentModel cqfAssessmentModel) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.USER_ID, cqfAssessmentModel.getUserId());
        propertyMap.put(Constants.ASSESSMENT_ID_KEY, cqfAssessmentModel.getAssessmentIdentifier());
        propertyMap.put(Constants.CONTENT_ID_KEY, cqfAssessmentModel.getContentId());
        propertyMap.put(Constants.VERSION_KEY, cqfAssessmentModel.getVersionKey());
        return cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_CQF_ASSESSMENT_DATA,
                propertyMap, null);
    }


    /**
     * Submits a CQF assessment.
     * <p>
     * This method processes the submit request, validates the data, and updates the assessment result.
     *
     * @param submitRequest The submit request data.
     * @param userAuthToken The user authentication token.
     * @param editMode      Whether the assessment is in edit mode.
     * @return The API response.
     */
    @Override
    public SBApiResponse submitCQFAssessment(Map<String, Object> submitRequest, String userAuthToken, boolean editMode) {
        logger.info("CQFAssessmentServiceImpl::submitCQFAssessment.. started");
        // Create the default API response
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.API_SUBMIT_ASSESSMENT);
        // Initialize the CQF assessment model
        CQFAssessmentModel cqfAssessmentModel = new CQFAssessmentModel();
        List<Map<String, Object>> sectionLevelsResults = new ArrayList<>();
        String errMsg;
        try {
            // Step-1 fetch userid
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                return handleUserIdDoesNotExist(outgoingResponse);
            }

            // Validate the submit assessment request
            errMsg = validateSubmitAssessmentRequest(submitRequest, userId, cqfAssessmentModel, userAuthToken, editMode);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(outgoingResponse, errMsg);
                return outgoingResponse;
            }
            String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
            String assessmentType = ((String) cqfAssessmentModel.getAssessmentHierarchy().get(Constants.ASSESSMENT_TYPE)).toLowerCase();
            // Process each hierarchy section
            for (Map<String, Object> hierarchySection : cqfAssessmentModel.getHierarchySectionList()) {
                String hierarchySectionId = (String) hierarchySection.get(Constants.IDENTIFIER);
                String userSectionId = "";
                Map<String, Object> userSectionData = new HashMap<>();
                for (Map<String, Object> sectionFromSubmitRequest : cqfAssessmentModel.getSectionListFromSubmitRequest()) {
                    userSectionId = (String) sectionFromSubmitRequest.get(Constants.IDENTIFIER);
                    if (userSectionId.equalsIgnoreCase(hierarchySectionId)) {
                        userSectionData = sectionFromSubmitRequest;
                        break;
                    }
                }
                hierarchySection.put(Constants.SCORE_CUTOFF_TYPE, assessmentType);
                List<Map<String, Object>> questionsListFromSubmitRequest = new ArrayList<>();
                if (userSectionData.containsKey(Constants.CHILDREN)
                        && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
                    questionsListFromSubmitRequest = objectMapper.convertValue(userSectionData.get(Constants.CHILDREN),
                            new TypeReference<List<Map<String, Object>>>() {
                            });
                }
                List<String> desiredKeys = Lists.newArrayList(Constants.IDENTIFIER);
                List<Object> questionsList = questionsListFromSubmitRequest.stream()
                        .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).collect(toList());
                List<String> questionsListFromAssessmentHierarchy = questionsList.stream()
                        .map(object -> Objects.toString(object, null)).collect(toList());
                Map<String, Object> result = new HashMap<>();
                Map<String, Object> questionSetDetailsMap = getParamDetailsForQTypes(hierarchySection, cqfAssessmentModel.getAssessmentHierarchy());
                if (assessmentType.equalsIgnoreCase(Constants.QUESTION_OPTION_WEIGHTAGE)) {
                    result.putAll(createResponseMapWithProperStructure(hierarchySection,
                            validateCQFAssessment(questionSetDetailsMap, questionsListFromAssessmentHierarchy,
                                    questionsListFromSubmitRequest, assessUtilServ.readQListfromCache(questionsListFromAssessmentHierarchy, assessmentIdFromRequest, editMode, userAuthToken))));
                    outgoingResponse.getResult().putAll(result);
                    sectionLevelsResults.add(result);
                }
            }
            Map<String, Object> result = new HashMap<>();
            result.put("result", sectionLevelsResults);
            String questionSetFromAssessmentString = (String) cqfAssessmentModel.getExistingAssessmentData().get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
            Map<String, Object> questionSetFromAssessment = null;
            if (StringUtils.isNotBlank(questionSetFromAssessmentString)) {
                questionSetFromAssessment = objectMapper.readValue(questionSetFromAssessmentString,
                        new TypeReference<Map<String, Object>>() {
                        });
            }
            writeDataToDatabase(submitRequest, userId, questionSetFromAssessment, result);
            return outgoingResponse;
        } catch (Exception e) {
            errMsg = String.format("Failed to process assessment submit request. Exception: ", e.getMessage());
            logger.error(errMsg, e);
            updateErrorDetails(outgoingResponse, errMsg);
        }
        return outgoingResponse;
    }

    /**
     * Writes data to the database after an assessment has been submitted.
     *
     * This method updates the assessment data in the database with the submitted response.
     * It also logs any errors that occur during the process.
     *
     * @param submitRequest The request object containing the submitted assessment data
     * @param userId The ID of the user who submitted the assessment
     * @param questionSetFromAssessment The question set from the assessment
     * @param result The result of the assessment submission
     */
    private void writeDataToDatabase(Map<String, Object> submitRequest, String userId,
                                     Map<String, Object> questionSetFromAssessment, Map<String, Object> result) {
        try {
            if (questionSetFromAssessment.get(Constants.START_TIME) != null) {
                Long existingAssessmentStartTime = (Long) questionSetFromAssessment.get(Constants.START_TIME);
                Timestamp startTime = new Timestamp(existingAssessmentStartTime);
                Map<String, Object> paramsMap = new HashMap<>();
                paramsMap.put(Constants.USER_ID, userId);
                paramsMap.put(Constants.ASSESSMENT_IDENTIFIER, submitRequest.get(Constants.IDENTIFIER));
                paramsMap.put(Constants.CONTENT_ID_KEY, submitRequest.get(Constants.CONTENT_ID_KEY));
                paramsMap.put(Constants.VERSION_KEY, submitRequest.get(Constants.VERSION_KEY));
                assessmentRepository.updateCQFAssesmentDataToDB(paramsMap, submitRequest, result, Constants.SUBMITTED,
                        startTime, null);
            }
        } catch (Exception e) {
            logger.error("Failed to write data for assessment submit response. Exception: ", e);
        }
    }

    /**
     * Validates a submit assessment request.
     * <p>
     * This method checks the validity of the submit request, reads the assessment hierarchy from cache,
     * checks if the primary category is practice question set or edit mode, reads the user submitted assessment records,
     * and validates the section details and question IDs.
     *
     * @param submitRequest      The submit request to be validated.
     * @param userId             The ID of the user submitting the assessment.
     * @param cqfAssessmentModel The CQF assessment model.
     * @param token              The token for the assessment.
     * @param editMode           Whether the assessment is in edit mode.
     * @return An error message if the validation fails, otherwise an empty string.
     * @throws Exception If an error occurs during validation.
     */
    private String validateSubmitAssessmentRequest(Map<String, Object> submitRequest, String userId, CQFAssessmentModel cqfAssessmentModel, String token, boolean editMode) throws Exception {
        submitRequest.put(Constants.USER_ID, userId);
        if (StringUtils.isEmpty((String) submitRequest.get(Constants.IDENTIFIER))) {
            return Constants.INVALID_ASSESSMENT_ID;
        }
        String assessmentIdFromRequest = (String) submitRequest.get(Constants.IDENTIFIER);
        cqfAssessmentModel.getAssessmentHierarchy().putAll(assessUtilServ.readAssessmentHierarchyFromCache(assessmentIdFromRequest, editMode, token));
        if (MapUtils.isEmpty(cqfAssessmentModel.getAssessmentHierarchy())) {
            return Constants.READ_ASSESSMENT_FAILED;
        }
        // Get the hierarchy section list and section list from submit request
        cqfAssessmentModel.getHierarchySectionList().addAll(objectMapper.convertValue(
                cqfAssessmentModel.getAssessmentHierarchy().get(Constants.CHILDREN),
                new TypeReference<List<Map<String, Object>>>() {
                }
        ));
        cqfAssessmentModel.getSectionListFromSubmitRequest().addAll(objectMapper.convertValue(
                submitRequest.get(Constants.CHILDREN),
                new TypeReference<List<Map<String, Object>>>() {
                }
        ));
        // Check if the primary category is practice question set or edit mode
        if (((String) (cqfAssessmentModel.getAssessmentHierarchy().get(Constants.PRIMARY_CATEGORY)))
                .equalsIgnoreCase(Constants.PRACTICE_QUESTION_SET) || editMode) {
            return "";
        }
        // Read the user submitted assessment records
        List<Map<String, Object>> existingDataList = readUserSubmittedAssessmentRecords(
                new CQFAssessmentModel(userId, submitRequest.get(Constants.IDENTIFIER).toString(), submitRequest.get(Constants.CONTENT_ID_KEY).toString(), submitRequest.get(Constants.VERSION_KEY).toString()));

        // Check if the existing data list is empty
        if (existingDataList.isEmpty()) {
            return Constants.USER_ASSESSMENT_DATA_NOT_PRESENT;
        } else {
            // Add the existing assessment data to the CQF assessment model
            cqfAssessmentModel.getExistingAssessmentData().putAll(existingDataList.get(0));
        }
        // Get the assessment start time
        Date assessmentStartTime = (Date) cqfAssessmentModel.getExistingAssessmentData().get(Constants.START_TIME);
        // Check if the assessment start time is null
        if (assessmentStartTime == null) {
            return Constants.READ_ASSESSMENT_START_TIME_FAILED;
        }
        // Calculate the expected duration and submission time
        int expectedDuration = (Integer) cqfAssessmentModel.getAssessmentHierarchy().get(Constants.EXPECTED_DURATION);
        Timestamp later = calculateAssessmentSubmitTime(expectedDuration,
                new Timestamp(assessmentStartTime.getTime()),
                Integer.parseInt(serverProperties.getUserAssessmentSubmissionDuration()));
        Timestamp submissionTime = new Timestamp(new Date().getTime());
        // Check if the submission time is before the expected duration
        int time = submissionTime.compareTo(later);
        if (time <= 0) {
            // Validate the section details
            List<String> desiredKeys = Lists.newArrayList(Constants.IDENTIFIER);
            List<Object> hierarchySectionIds = cqfAssessmentModel.getHierarchySectionList().stream()
                    .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).collect(toList());
            List<Object> submitSectionIds = cqfAssessmentModel.getSectionListFromSubmitRequest().stream()
                    .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).collect(toList());
            if (!new HashSet<>(hierarchySectionIds).containsAll(submitSectionIds)) {
                return Constants.WRONG_SECTION_DETAILS;
            } else {
                // Validate the question IDs
                String areQuestionIdsSame = validateIfQuestionIdsAreSame(
                        cqfAssessmentModel.getSectionListFromSubmitRequest(), desiredKeys, cqfAssessmentModel.getExistingAssessmentData());
                if (!areQuestionIdsSame.isEmpty())
                    return areQuestionIdsSame;
            }
        } else {
            // Return an error if the submission time has expired
            return Constants.ASSESSMENT_SUBMIT_EXPIRED;
        }
        return "";
    }


    /**
     * Validates if the question IDs from the submit request are the same as the question IDs from the assessment hierarchy.
     * <p>
     * This method reads the question set from the assessment, extracts the question IDs from the assessment hierarchy,
     * and compares them with the question IDs from the submit request.
     *
     * @param sectionListFromSubmitRequest The list of sections from the submit request.
     * @param desiredKeys                  The list of desired keys to extract from the sections.
     * @param existingAssessmentData       The existing assessment data.
     * @return An error message if the validation fails, otherwise an empty string.
     */
    private String validateIfQuestionIdsAreSame(List<Map<String, Object>> sectionListFromSubmitRequest, List<String> desiredKeys,
                                                Map<String, Object> existingAssessmentData) {
        String questionSetFromAssessmentString = getQuestionSetFromAssessment(existingAssessmentData);
        if (StringUtils.isBlank(questionSetFromAssessmentString)) {
            return Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED;
        }

        Map<String, Object> questionSetFromAssessment = null;
        try {
            questionSetFromAssessment = objectMapper.readValue(questionSetFromAssessmentString,
                    new TypeReference<Map<String, Object>>() {
                    });
        } catch (IOException e) {
            logger.error("Failed to parse question set from assessment. Exception: ", e);
            return Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED;
        }
        if (MapUtils.isEmpty(questionSetFromAssessment)) {
            return Constants.ASSESSMENT_SUBMIT_QUESTION_READ_FAILED;
        }
        List<Map<String, Object>> sections = objectMapper.convertValue(
                questionSetFromAssessment.get(Constants.CHILDREN),
                new TypeReference<List<Map<String, Object>>>() {
                }
        );
        List<String> desiredKey = Lists.newArrayList(Constants.CHILD_NODES);
        List<Object> questionList = sections.stream()
                .flatMap(x -> desiredKey.stream().filter(x::containsKey).map(x::get)).collect(toList());
        List<String> questionIdsFromAssessmentHierarchy = new ArrayList<>();
        for (Object question : questionList) {
            questionIdsFromAssessmentHierarchy.addAll(objectMapper.convertValue(question,
                    new TypeReference<List<String>>() {
                    }
            ));
        }
        List<String> userQuestionIdsFromSubmitRequest = getUserQuestionIdsFromSubmitRequest(sectionListFromSubmitRequest, desiredKeys);


        if (!new HashSet<>(questionIdsFromAssessmentHierarchy).containsAll(userQuestionIdsFromSubmitRequest)) {
            return Constants.ASSESSMENT_SUBMIT_INVALID_QUESTION;
        }
        return "";
    }


    /**
     * Retrieves the question set from the existing assessment data.
     * <p>
     * This method extracts the question set from the assessment data using the assessment read response key.
     *
     * @param existingAssessmentData The existing assessment data.
     * @return The question set from the assessment data.
     */
    private String getQuestionSetFromAssessment(Map<String, Object> existingAssessmentData) {
        // Extract the question set from the assessment data using the assessment read response key
        return (String) existingAssessmentData.get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
    }


    /**
     * Retrieves the user question IDs from the submit request.
     * <p>
     * This method extracts the question IDs from the section list in the submit request.
     *
     * @param sectionListFromSubmitRequest The section list from the submit request.
     * @param desiredKeys                  The desired keys to extract from the section list.
     * @return The list of user question IDs.
     */
    private List<String> getUserQuestionIdsFromSubmitRequest(List<Map<String, Object>> sectionListFromSubmitRequest, List<String> desiredKeys) {
        List<Map<String, Object>> questionsListFromSubmitRequest = new ArrayList<>();
        for (Map<String, Object> userSectionData : sectionListFromSubmitRequest) {
            if (userSectionData.containsKey(Constants.CHILDREN)
                    && !ObjectUtils.isEmpty(userSectionData.get(Constants.CHILDREN))) {
                questionsListFromSubmitRequest
                        .addAll(objectMapper.convertValue(
                                userSectionData.get(Constants.CHILDREN),
                                new TypeReference<List<Map<String, Object>>>() {
                                }
                        ));
            }
        }
        return questionsListFromSubmitRequest.stream()
                .flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get))
                .map(x -> (String) x)
                .collect(toList());

    }


    /**
     * Creates a response map with the proper structure.
     * <p>
     * This method takes a hierarchy section and a result map as input, and returns a new map with the required structure.
     *
     * @param hierarchySection The hierarchy section to extract data from.
     * @param resultMap        The result map to extract additional data from.
     * @return The response map with the proper structure.
     */
    public Map<String, Object> createResponseMapWithProperStructure(Map<String, Object> hierarchySection,
                                                                    Map<String, Object> resultMap) {
        Map<String, Object> sectionLevelResult = new HashMap<>();
        sectionLevelResult.put(Constants.IDENTIFIER, hierarchySection.get(Constants.IDENTIFIER));
        sectionLevelResult.put(Constants.OBJECT_TYPE, hierarchySection.get(Constants.OBJECT_TYPE));
        sectionLevelResult.put(Constants.PRIMARY_CATEGORY, hierarchySection.get(Constants.PRIMARY_CATEGORY));
        sectionLevelResult.put(Constants.PASS_PERCENTAGE, hierarchySection.get(Constants.MINIMUM_PASS_PERCENTAGE));
        sectionLevelResult.put(Constants.NAME, hierarchySection.get(Constants.NAME));
        sectionLevelResult.put(Constants.MAX_USER_SCORE_FOR_SECTION, resultMap.get(Constants.MAX_USER_SCORE_FOR_SECTION));
        sectionLevelResult.put(Constants.MAX_WEIGHTED_SCORE_FOR_SECTION, resultMap.get(Constants.MAX_WEIGHTED_SCORE_FOR_SECTION));
        sectionLevelResult.put(Constants.USER_WEIGHTED_SCORE_FOR_SECTION,resultMap.get(Constants.USER_WEIGHTED_SCORE_FOR_SECTION));
        return sectionLevelResult;
    }

    /**
     * @param questionSetDetailsMap a map containing details about the question set.
     * @param originalQuestionList  a list of original question identifiers.
     * @param userQuestionList      a list of maps where each map represents a user's question with its details.
     * @param questionMap           a map containing additional question-related information.
     * @return a map with validation results and resultMap.
     */
    public Map<String, Object> validateCQFAssessment(Map<String, Object> questionSetDetailsMap, List<String> originalQuestionList,
                                                     List<Map<String, Object>> userQuestionList, Map<String, Object> questionMap) {
        try {
            Integer blank = 0;
            double userCriteriaScore = 0.0;
            double maxWeightedScoreForQn = 0.0;
            double maxWeightedScoreForSection;
            double maxUserScoreForSection;
            double userWeightedScoreForSection;
            double totalUserCriteriaScoreForSection = 0.0;

            String assessmentType = (String) questionSetDetailsMap.get(Constants.ASSESSMENT_TYPE);
            Map<String, Object> resultMap = new HashMap<>();
            Map<String, Object> optionWeightages = new HashMap<>();
            Map<String, Object> maxMarksForQuestion = new HashMap<>();
            if (assessmentType.equalsIgnoreCase(Constants.QUESTION_OPTION_WEIGHTAGE)) {
                optionWeightages = getOptionWeightages(originalQuestionList, questionMap);
                maxMarksForQuestion = getMaxMarksForQustions(originalQuestionList, questionMap);
            }
            for (Map<String, Object> question : userQuestionList) {
                List<String> marked = new ArrayList<>();
                handleqTypeQuestion(question, marked);
                if (CollectionUtils.isEmpty(marked)) {
                    blank++;
                    question.put(Constants.RESULT, Constants.BLANK);
                } else {
                    userCriteriaScore = calculateScoreForOptionWeightage(question, assessmentType, optionWeightages, userCriteriaScore, marked);
                    String identifier = question.get(Constants.IDENTIFIER).toString();
                    maxWeightedScoreForQn = maxWeightedScoreForQn + (double) maxMarksForQuestion.get(identifier);
                    totalUserCriteriaScoreForSection =totalUserCriteriaScoreForSection + userCriteriaScore;
                }
            }
            maxWeightedScoreForSection = maxWeightedScoreForQn * ((double) questionSetDetailsMap.get(Constants.SECTION_WEIGHTAGE) / 100);
            userWeightedScoreForSection = userCriteriaScore * ((double) questionSetDetailsMap.get(Constants.SECTION_WEIGHTAGE) / 100);
            if (userWeightedScoreForSection > 0) {
                maxUserScoreForSection = userWeightedScoreForSection / maxWeightedScoreForSection;
                resultMap.put(Constants.MAX_WEIGHTED_SCORE_FOR_SECTION, maxWeightedScoreForSection);
                resultMap.put(Constants.USER_WEIGHTED_SCORE_FOR_SECTION, userWeightedScoreForSection);
                resultMap.put(Constants.MAX_USER_SCORE_FOR_SECTION, maxUserScoreForSection);
                resultMap.put(Constants.TOTAL_USER_CRITERIA_SCORE_FOR_SECTION, totalUserCriteriaScoreForSection);
                resultMap.put(Constants.BLANK, blank);
            }
            return resultMap;
        } catch (Exception ex) {
            logger.error("Error when verifying assessment. Error : ", ex);
        }
        return new HashMap<>();
    }

    /**
     * Retrieves option weightages for a list of questions corresponding to their options.
     *
     * @param questions   the list of questionIDs/doIds.
     * @param questionMap the map containing questions/Question Level details.
     * @return a map containing Identifier mapped to their option and option weightages.
     */
    private Map<String, Object> getOptionWeightages(List<String> questions, Map<String, Object> questionMap) {
        logger.info("Retrieving option weightages for questions based on the options...");
        Map<String, Object> ret = new HashMap<>();
        for (String questionId : questions) {
            Map<String, Object> optionWeightage = new HashMap<>();
            Map<String, Object> question = objectMapper.convertValue(questionMap.get(questionId), new TypeReference<Map<String, Object>>() {
            });
            if (question.containsKey(Constants.QUESTION_TYPE)) {
                String questionType = ((String) question.get(Constants.QUESTION_TYPE)).toLowerCase();
                Map<String, Object> editorStateObj = objectMapper.convertValue(question.get(Constants.EDITOR_STATE), new TypeReference<Map<String, Object>>() {
                });
                List<Map<String, Object>> options = objectMapper.convertValue(editorStateObj.get(Constants.OPTIONS), new TypeReference<List<Map<String, Object>>>() {
                });
                switch (questionType) {
                    case Constants.MCQ_SCA:
                    case Constants.MCQ_MCA:
                    case Constants.MCQ_MCA_W:
                        for (Map<String, Object> option : options) {
                            Map<String, Object> valueObj = objectMapper.convertValue(option.get(Constants.VALUE), new TypeReference<Map<String, Object>>() {
                            });
                            optionWeightage.put(valueObj.get(Constants.VALUE).toString(), option.get(Constants.ANSWER));
                        }
                        break;
                    default:
                        break;
                }
            }
            ret.put(question.get(Constants.IDENTIFIER).toString(), optionWeightage);
        }
        logger.info("Option weightages retrieved successfully.");
        return ret;
    }

    /**
     * Retrieves the maximum marks for a list of questions.
     * <p>
     * This method takes a list of question IDs and a question map as input, and returns a map with the maximum marks for each question.
     *
     * @param questions   The list of question IDs.
     * @param questionMap The map of questions with their details.
     * @return A map with the maximum marks for each question.
     */
    private Map<String, Object> getMaxMarksForQustions(List<String> questions, Map<String, Object> questionMap) {
        logger.info("Retrieving max weightages for questions based on the questions...");
        Map<String, Object> ret = new HashMap<>();
        for (String questionId : questions) {
            double maxMarks = 0;
            Map<String, Object> question = objectMapper.convertValue(questionMap.get(questionId), new TypeReference<Map<String, Object>>() {
            });
            if (question.containsKey(Constants.QUESTION_TYPE)) {
                String questionType = ((String) question.get(Constants.QUESTION_TYPE)).toLowerCase();
                switch (questionType) {
                    case Constants.MCQ_SCA:
                    case Constants.MCQ_MCA:
                    case Constants.MCQ_MCA_W:
                        maxMarks = (double) question.get(Constants.TOTAL_MARKS);
                        break;
                    default:
                        break;
                }
            }
            ret.put(question.get(Constants.IDENTIFIER).toString(), maxMarks);
        }
        logger.info("max weightages retrieved successfully.");
        return ret;
    }


    /**
     * Handles the question type and retrieves the marked indices for each question.
     * <p>
     * This method takes a question map, a list of marked indices, and an assessment type as input.
     * It checks if the question has a question type and retrieves the marked indices based on the question type.
     *
     * @param question       The question map.
     * @param marked         The list of marked indices.
     */
    private void handleqTypeQuestion(Map<String, Object> question, List<String> marked) {
        if (question.containsKey(Constants.QUESTION_TYPE)) {
            String questionType = ((String) question.get(Constants.QUESTION_TYPE)).toLowerCase();
            Map<String, Object> editorStateObj = objectMapper.convertValue(question.get(Constants.EDITOR_STATE), new TypeReference<Map<String, Object>>() {
            });
            List<Map<String, Object>> options = objectMapper.convertValue(editorStateObj.get(Constants.OPTIONS), new TypeReference<List<Map<String, Object>>>() {
            });
            getMarkedIndexForEachQuestion(questionType, options, marked);
        }
    }

    /**
     * Gets index for each question based on the question type.
     *
     * @param questionType   the type of question.
     * @param options        the list of options.
     * @param marked         the list to store marked indices.
     */
    private void getMarkedIndexForEachQuestion(String questionType, List<Map<String, Object>> options, List<String> marked) {
        logger.info("Getting marks or index for each question...");
        if (questionType.equalsIgnoreCase(Constants.MCQ_MCA_W)) {
            getMarkedIndexForOptionWeightAge(options, marked);
        }
        logger.info("Marks or index retrieved successfully.");
    }


    /**
     * Calculates the score for option weightage based on the given question, assessment type, option weightages, section marks, and marked indices.
     * <p>
     * This method takes a question map, an assessment type, a map of option weightages, a section marks value, and a list of marked indices as input.
     * It calculates the score for option weightage based on the assessment type and returns the updated section marks value.
     *
     * @param question         The question map.
     * @param assessmentType   The assessment type.
     * @param optionWeightages The map of option weightages.
     * @param sectionMarks     The section marks value.
     * @param marked           The list of marked indices.
     * @return The updated section marks value.
     */
    private Double calculateScoreForOptionWeightage(Map<String, Object> question, String assessmentType, Map<String, Object> optionWeightages, Double sectionMarks, List<String> marked) {
        if (assessmentType.equalsIgnoreCase(Constants.OPTION_WEIGHTAGE)) {
            String identifier = question.get(Constants.IDENTIFIER).toString();
            Map<String, Object> optionWeightageMap = objectMapper.convertValue(optionWeightages.get(identifier), new TypeReference<Map<String, Object>>() {
            });
            for (Map.Entry<String, Object> optionWeightAgeFromOptions : optionWeightageMap.entrySet()) {
                String submittedQuestionSetIndex = marked.get(0);
                if (submittedQuestionSetIndex.equals(optionWeightAgeFromOptions.getKey())) {
                    sectionMarks = sectionMarks + Integer.parseInt((String) optionWeightAgeFromOptions.getValue());
                }
            }
        }
        return sectionMarks;
    }


    /**
     * Gets index for each question based on the question type.
     *
     * @param options the list of options.
     */
    private void getMarkedIndexForOptionWeightAge(List<Map<String, Object>> options, List<String> marked) {
        logger.info("Processing marks for option weightage...");
        for (Map<String, Object> option : options) {
            String submittedQuestionSetIndex = (String) option.get(Constants.INDEX);
            marked.add(submittedQuestionSetIndex);
        }
        logger.info("Marks for option weightage processed successfully.");
    }


    /**
     * Retrieves the parameter details for question types based on the given assessment hierarchy.
     *
     * @param assessmentHierarchy a map containing the assessment hierarchy details.
     * @return a map containing the parameter details for the question types.
     */
    private Map<String, Object> getParamDetailsForQTypes(Map<String, Object> hierarchySection, Map<String, Object> assessmentHierarchy) {
        logger.info("Starting getParamDetailsForQTypes with assessmentHierarchy: {}", assessmentHierarchy);
        Map<String, Object> questionSetDetailsMap = new HashMap<>();
        String assessmentType = (String) assessmentHierarchy.get(Constants.ASSESSMENT_TYPE);
        questionSetDetailsMap.put(Constants.ASSESSMENT_TYPE, assessmentType);
        questionSetDetailsMap.put(Constants.MINIMUM_PASS_PERCENTAGE, assessmentHierarchy.get(Constants.MINIMUM_PASS_PERCENTAGE));
        questionSetDetailsMap.put(Constants.TOTAL_MARKS, hierarchySection.get(Constants.TOTAL_MARKS));
        logger.info("Completed getParamDetailsForQTypes with result: {}", questionSetDetailsMap);
        return questionSetDetailsMap;
    }

    /**
     * Reads the CQF assessment result for a given user.
     * <p>
     * This method takes in a request map and a user authentication token,
     * validates the request, and returns the assessment result.
     *
     * @param request       The request map containing the assessment details.
     * @param userAuthToken The user authentication token.
     * @return The assessment result response.
     */
    public SBApiResponse readCQFAssessmentResult(Map<String, Object> request, String userAuthToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_READ_ASSESSMENT_RESULT);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            String errMsg = validateAssessmentReadResult(request);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> requestBody = objectMapper.convertValue(
                    request.get(Constants.REQUEST),
                    new TypeReference<Map<String, Object>>() {
                    }
            );

            List<Map<String, Object>> existingDataList = readUserSubmittedAssessmentRecords(new CQFAssessmentModel(
                    userId, requestBody.get(Constants.ASSESSMENT_ID).toString(), requestBody.get(Constants.CONTENT_ID_KEY).toString(), requestBody.get(Constants.VERSION_KEY).toString()));
            if (existingDataList.isEmpty()) {
                updateErrorDetails(response, Constants.USER_ASSESSMENT_DATA_NOT_PRESENT, HttpStatus.BAD_REQUEST);
                return response;
            }

            String statusOfLatestObject = (String) existingDataList.get(0).get(Constants.STATUS);
            if (!Constants.SUBMITTED.equalsIgnoreCase(statusOfLatestObject)) {
                response.getResult().put(Constants.STATUS_IS_IN_PROGRESS, true);
                return response;
            }

            String latestResponse = (String) existingDataList.get(0).get(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY);
            if (StringUtils.isNotBlank(latestResponse)) {
                response.putAll(objectMapper.readValue(latestResponse, new TypeReference<Map<String, Object>>() {
                }));
            }
        } catch (Exception e) {
            String errMsg = String.format("Failed to process Assessment read response. Excption: %s", e.getMessage());
            updateErrorDetails(response, errMsg, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }


    /**
     * Updates the error details in the response object.
     * <p>
     * This method takes in a response object, an error message, and a response code,
     * and updates the response object with the error details.
     *
     * @param response     The response object to be updated.
     * @param errMsg       The error message to be set in the response.
     * @param responseCode The response code to be set in the response.
     */
    private void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }


    /**
     * Validates the assessment read result request.
     * <p>
     * This method checks if the request is valid and contains all the required attributes.
     *
     * @param request The request map to be validated.
     * @return The error message if the request is invalid, or an empty string if the request is valid.
     */
    private String validateAssessmentReadResult(Map<String, Object> request) {
        String errMsg = "";
        if (MapUtils.isEmpty(request) || !request.containsKey(Constants.REQUEST)) {
            return Constants.INVALID_REQUEST;
        }

        Map<String, Object> requestBody = objectMapper.convertValue(
                request.get(Constants.REQUEST),
                new TypeReference<Map<String, Object>>() {
                }
        );
        if (MapUtils.isEmpty(requestBody)) {
            return Constants.INVALID_REQUEST;
        }
        List<String> missingAttribs = new ArrayList<>();
        if (!requestBody.containsKey(Constants.ASSESSMENT_ID_KEY)
                || StringUtils.isBlank((String) requestBody.get(Constants.ASSESSMENT_ID_KEY))) {
            missingAttribs.add(Constants.ASSESSMENT_ID_KEY);
        }

        if (!requestBody.containsKey(Constants.BATCH_ID)
                || StringUtils.isBlank((String) requestBody.get(Constants.BATCH_ID))) {
            missingAttribs.add(Constants.BATCH_ID);
        }

        if (!requestBody.containsKey(Constants.COURSE_ID)
                || StringUtils.isBlank((String) requestBody.get(Constants.COURSE_ID))) {
            missingAttribs.add(Constants.COURSE_ID);
        }

        if (!missingAttribs.isEmpty()) {
            errMsg = "One or more mandatory fields are missing in Request. Mandatory fields are : "
                    + missingAttribs.toString();
        }
        return errMsg;
    }
}