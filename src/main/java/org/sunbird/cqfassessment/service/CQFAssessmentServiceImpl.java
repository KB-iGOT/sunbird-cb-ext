package org.sunbird.cqfassessment.service;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
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
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.*;
import org.sunbird.cqfassessment.model.CQFAssessmentModel;

import java.io.IOException;
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

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    IndexerService indexerService;

    @Autowired
    CbExtServerProperties serverConfig;
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
     * @param authToken   the authentication token for the request
     * @param requestBody Request body of the request
     * @return the API response containing the list of assessments
     */
    @Override
    public SBApiResponse listCQFAssessments(String authToken,  Map<String, Object> requestBody) {
        logger.info("CQFAssessmentServiceImpl::listCQFAssessments... Started");
        List<Map<String, Object>> resultArray = new ArrayList<>();
        Map<String, Object> result;
        Map<String, Object> resultResp = new HashMap<>();
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.CQF_API_LIST_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(response, HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        SearchResponse searchResponse;
        try {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchSourceBuilder.sort(new FieldSortBuilder(requestBody.get(Constants.FIELD).toString()).order(SortOrder.DESC));
            searchSourceBuilder.from((Integer.parseInt(requestBody.get(Constants.CURRENT_PAGE).toString()) - 1) * Integer.parseInt(requestBody.get(Constants.PAGE_SIZE).toString()));
            searchSourceBuilder.size(Integer.parseInt(requestBody.get(Constants.PAGE_SIZE).toString()));
            if(indexerService.isIndexPresent(serverProperties.getQuestionSetHierarchyIndex())) {
                searchResponse = indexerService.getEsResult(serverProperties.getQuestionSetHierarchyIndex(), serverConfig.getEsProfileIndexType(), searchSourceBuilder, false);
                for (SearchHit hit : searchResponse.getHits()) {
                    result = hit.getSourceAsMap();
                    resultArray.add(result);
                }
            }
        } catch (IOException e) {
            logger.error(String.format("Failed to process the cqfquestionList search. %s", e.getMessage()));
        }
        resultResp.put(Constants.CQF_ASSESSMENT_DATA, resultArray);
        response.getResult().putAll(resultResp);
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
        return processAssessment(assessmentAllDetail,  response, existingDataList, cqfAssessmentModel);
    }


    /**
     * Processes the assessment based on whether it's a first-time assessment or an existing one.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.

     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param existingDataList    A list of maps containing existing assessment data.
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @return The SBApiResponse object containing the assessment results.
     */
    public SBApiResponse processAssessment(Map<String, Object> assessmentAllDetail, SBApiResponse response, List<Map<String, Object>> existingDataList, CQFAssessmentModel cqfAssessmentModel) {
        if (existingDataList.isEmpty()) {
            return handleFirstTimeAssessment(assessmentAllDetail, response, cqfAssessmentModel);
        } else {
            return handleExistingAssessment(assessmentAllDetail,  response, existingDataList);
        }
    }

    /**
     * Handles the first-time assessment by preparing the assessment data and updating it to the database.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @return The SBApiResponse object containing the assessment results.
     */
    private SBApiResponse handleFirstTimeAssessment(Map<String, Object> assessmentAllDetail, SBApiResponse response, CQFAssessmentModel cqfAssessmentModel) {
        logger.info("Assessment read first time for user.");
        Map<String, Object> assessmentData = prepareAssessmentData(assessmentAllDetail);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);
        Map<String, Object> questionSetMap = objectMapper.convertValue(response.getResult().get(Constants.QUESTION_SET), new TypeReference<Map<String, Object>>() {
        });
        if (Boolean.FALSE.equals(updateAssessmentDataToDB(cqfAssessmentModel, questionSetMap))) {
            updateErrorDetails(response, Constants.ASSESSMENT_DATA_START_TIME_NOT_UPDATED);
        }
        return response;
    }


    /**
     * Prepares the assessment data by reading the assessment level data and adding the start and end times.
     *
     * @param assessmentAllDetail The map containing the assessment details.
     * @return The prepared assessment data.
     */
    private Map<String, Object> prepareAssessmentData(Map<String, Object> assessmentAllDetail) {
        return readAssessmentLevelData(assessmentAllDetail);
    }


    /**
     * Updates the assessment data to the database by adding the user's CQF assessment data.
     *
     * @param cqfAssessmentModel  A CQFAssessmentModel object representing the assessment.
     * @param questionSetMap      A map containing the question set data.
     * @return True if the assessment data was updated successfully, false otherwise.
     */
    private Boolean updateAssessmentDataToDB(CQFAssessmentModel cqfAssessmentModel, Map<String, Object> questionSetMap) {
        return assessmentRepository.addUserCQFAssesmentDataToDB(cqfAssessmentModel,
                questionSetMap,
                Constants.NOT_SUBMITTED);

    }

    /**
     * Handles an existing assessment by determining whether it is still ongoing, can be reattempted, or has expired.
     *
     * @param assessmentAllDetail A map containing all the details of the assessment.
     * @param response            The SBApiResponse object to be populated with the assessment results.
     * @param existingDataList    A list of maps containing the existing assessment data.
     * @return The SBApiResponse object containing the assessment results, or null if the assessment is not handled.
     */
    private SBApiResponse handleExistingAssessment(Map<String, Object> assessmentAllDetail, SBApiResponse response, List<Map<String, Object>> existingDataList) {
        logger.info("Assessment read... user has details... ");
        String status = (String) existingDataList.get(0).get(Constants.STATUS);
        if (isAssessmentStillOngoing(status)) {
            return handleOngoingAssessment(existingDataList, response);
        }else{
            return handleAssessmentRetryOrExpired(assessmentAllDetail, response);
        }
    }

    /**
     * Checks if the assessment is still ongoing based on the start time, end time, and status.
     *
     * @param status                             The status of the assessment.
     * @return True if the assessment is still ongoing, false otherwise.
     */
    private boolean isAssessmentStillOngoing( String status) {
        return  Constants.NOT_SUBMITTED.equalsIgnoreCase(status);
    }

    /**
     * Handles the case where the assessment is still ongoing.
     *
     * @param existingDataList                   The list of existing assessment data.
     * @param response                           The API response object.
     * @return The API response object with the updated question set.
     */
    private SBApiResponse handleOngoingAssessment(List<Map<String, Object>> existingDataList, SBApiResponse response) {
        String questionSetFromAssessmentString = (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE_KEY);
        Map<String, Object> questionSetFromAssessment = new Gson().fromJson(
                questionSetFromAssessmentString, new TypeToken<HashMap<String, Object>>() {
                }.getType());
        response.getResult().put(Constants.QUESTION_SET, questionSetFromAssessment);
        return response;
    }



    private SBApiResponse handleAssessmentRetryOrExpired(Map<String, Object> assessmentAllDetail, SBApiResponse response) {
        logger.info("Incase the assessment is submitted before the end time, or the endtime has exceeded, read assessment freshly ");

        Map<String, Object> assessmentData = readAssessmentLevelData(assessmentAllDetail);
        response.getResult().put(Constants.QUESTION_SET, assessmentData);
        return response;
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
            List<String> childNodeList = questions.stream()
                    .map(question -> (String) question.get(Constants.IDENTIFIER))
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
                    sectionLevelsResults.add(result);
                }
            }
            Map<String, Object> result = calculateSectionFinalResults(sectionLevelsResults);
            outgoingResponse.getResult().putAll(result);
            writeDataToDatabase(submitRequest, userId, result);
            return outgoingResponse;
        } catch (Exception e) {
            errMsg = String.format("Failed to process assessment submit request. Exception: ", e.getMessage());
            logger.error(errMsg, e);
            updateErrorDetails(outgoingResponse, errMsg);
            return outgoingResponse;
        }
    }

    /**
     * Writes data to the database after an assessment has been submitted.
     *
     * This method updates the assessment data in the database with the submitted response.
     * It also logs any errors that occur during the process.
     *
     * @param submitRequest The request object containing the submitted assessment data
     * @param userId The ID of the user who submitted the assessment
     * @param result The result of the assessment submission
     */
    private void writeDataToDatabase(Map<String, Object> submitRequest, String userId,
                                     Map<String, Object> result) {
        try {
            Map<String, Object> paramsMap = new HashMap<>();
            paramsMap.put(Constants.USER_ID, userId);
            paramsMap.put(Constants.ASSESSMENT_IDENTIFIER, submitRequest.get(Constants.IDENTIFIER));
            paramsMap.put(Constants.CONTENT_ID_KEY, submitRequest.get(Constants.CONTENT_ID_KEY));
            paramsMap.put(Constants.VERSION_KEY, submitRequest.get(Constants.VERSION_KEY));
            assessmentRepository.updateCQFAssesmentDataToDB(paramsMap, submitRequest, result, Constants.SUBMITTED,
                     null);
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
     */
    private String validateSubmitAssessmentRequest(Map<String, Object> submitRequest, String userId, CQFAssessmentModel cqfAssessmentModel, String token, boolean editMode) {
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
        sectionLevelResult.put(Constants.OVERALL_SECTION_PERCENTAGE_SCORE, resultMap.get(Constants.OVERALL_SECTION_PERCENTAGE_SCORE));
        sectionLevelResult.put(Constants.ACHIEVED_PERCENTAGE_SCORE, resultMap.get(Constants.ACHIEVED_PERCENTAGE_SCORE));
        sectionLevelResult.put(Constants.SECTION_LEVEL_PERCENTAGE, resultMap.get(Constants.SECTION_LEVEL_PERCENTAGE));
        sectionLevelResult.put(Constants.ACHIEVED_MARKS_FOR_SECTION,resultMap.get(Constants.ACHIEVED_MARKS_FOR_SECTION));
        sectionLevelResult.put(Constants.TOTAL_MARKS_FOR_SECTION,resultMap.get(Constants.TOTAL_MARKS_FOR_SECTION));
        sectionLevelResult.put(Constants.CHILDREN, resultMap.get(Constants.CHILDREN));
        sectionLevelResult.put(Constants.SECTION_RESULT, resultMap.get(Constants.SECTION_RESULT));
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
            double achievedMarksPerQn;
            double achievedMarksForSection = 0.0;
            int maxMarksForQn = 0;
            int totalMarksForSection = 0;
            double overallSectionPercentageScore;
            double achievedPercentageScore;
            double sectionLevelPercentage;
            String assessmentType = (String) questionSetDetailsMap.get(Constants.ASSESSMENT_TYPE);
            Map<String, Object> resultMap = new HashMap<>();
            Map<String, Object> optionWeightages = new HashMap<>();
            Map<String, Object> maxMarksForQuestion = new HashMap<>();
            Map<String,Integer> optionValueNotApplicableForQuestion = new HashMap<>();
            int minimumPassPercentage = 0;
            if (questionSetDetailsMap.get(Constants.MINIMUM_PASS_PERCENTAGE) != null) {
                minimumPassPercentage = (int) questionSetDetailsMap.get(Constants.MINIMUM_PASS_PERCENTAGE);
            }
            if (assessmentType.equalsIgnoreCase(Constants.QUESTION_OPTION_WEIGHTAGE)) {
                optionWeightages = getOptionWeightages(originalQuestionList, questionMap);
                maxMarksForQuestion = getMaxMarksForQustions(optionWeightages);
                processNotApplicableValueInOptions(questionMap, optionValueNotApplicableForQuestion);
            }
            for (Map<String, Object> question : userQuestionList) {
                List<String> marked = new ArrayList<>();
                handleqTypeQuestion(question, marked);
                if (CollectionUtils.isEmpty(marked)) {
                    blank++;
                    question.put(Constants.RESULT, Constants.BLANK);
                } else {
                    achievedMarksPerQn = 0.0;
                    achievedMarksPerQn = calculateScoreForOptionWeightage(question, assessmentType, optionWeightages, achievedMarksPerQn, marked);
                    String identifier = question.get(Constants.IDENTIFIER).toString();
                    maxMarksForQn =  (int) maxMarksForQuestion.get(identifier);
                    achievedMarksForSection = achievedMarksForSection + achievedMarksPerQn;
                    if(!marked.get(0).equalsIgnoreCase(optionValueNotApplicableForQuestion.get(identifier).toString())) {
                        totalMarksForSection = totalMarksForSection + maxMarksForQn;
                    }
                    question.put(Constants.ACQUIRED_SCORE,achievedMarksPerQn);
                }
            }
            overallSectionPercentageScore = totalMarksForSection * ((double) questionSetDetailsMap.get(Constants.SECTION_WEIGHTAGE) / 100);
            achievedPercentageScore = achievedMarksForSection * ((double) questionSetDetailsMap.get(Constants.SECTION_WEIGHTAGE) / 100);
            if (totalMarksForSection >= 0) {
                sectionLevelPercentage = (achievedMarksForSection / totalMarksForSection) * 100;
                resultMap.put(Constants.SECTION_LEVEL_PERCENTAGE, sectionLevelPercentage);
            }
            resultMap.put(Constants.OVERALL_SECTION_PERCENTAGE_SCORE, overallSectionPercentageScore);
            resultMap.put(Constants.ACHIEVED_PERCENTAGE_SCORE, achievedPercentageScore);
            resultMap.put(Constants.ACHIEVED_MARKS_FOR_SECTION,achievedMarksForSection);
            resultMap.put(Constants.TOTAL_MARKS_FOR_SECTION,totalMarksForSection);
            resultMap.put(Constants.CHILDREN, userQuestionList);
            resultMap.put(Constants.BLANK, blank);
            computeSectionResults(achievedMarksForSection, totalMarksForSection, minimumPassPercentage, resultMap);
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
     * Retrieves the maximum marks for each question based on the provided option weightages.
     * <p>
     * This method iterates through the option weightages, parses the integer values, and keeps track of the maximum marks.
     *
     * @param optionWeightages A map of question identifiers to their corresponding weightages.
     * @return A map of question identifiers to their maximum marks.
     */
    public Map<String, Object> getMaxMarksForQustions(Map<String, Object> optionWeightages) {
        logger.info("Retrieving max weightages for questions based on the questions...");
        Map<String, Object> ret = new HashMap<>();
        for (Map.Entry<String, Object> entry : optionWeightages.entrySet()) {
            String identifier = entry.getKey();
            int maxMarks = 0;
            Map<String, Integer> weightages = objectMapper.convertValue(entry.getValue(), new TypeReference<Map<String, Integer>>() {});
            for (Map.Entry<String, Integer> marksMap : weightages.entrySet()) {
                int marks = 0;
                try {
                    marks = Integer.parseInt(String.valueOf(marksMap.getValue()));
                } catch (NumberFormatException e) {
                    logger.info("Invalid integer value: " + marksMap.getValue());
                }
                if (marks > maxMarks) {
                    maxMarks = marks;
                }
            }
            ret.put(identifier, maxMarks);
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
        if (assessmentType.equalsIgnoreCase(Constants.QUESTION_OPTION_WEIGHTAGE)) {
            String identifier = question.get(Constants.IDENTIFIER).toString();
            Map<String, Object> optionWeightageMap = objectMapper.convertValue(optionWeightages.get(identifier), new TypeReference<Map<String, Object>>() {
            });
            for (Map.Entry<String, Object> optionWeightAgeFromOptions : optionWeightageMap.entrySet()) {
                String submittedQuestionSetIndex = marked.get(0);
                if (submittedQuestionSetIndex.equals(optionWeightAgeFromOptions.getKey())) {
                    sectionMarks = sectionMarks + Integer.parseInt(optionWeightAgeFromOptions.getValue().toString());
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
        questionSetDetailsMap.put(Constants.MINIMUM_PASS_PERCENTAGE, hierarchySection.get(Constants.MINIMUM_PASS_PERCENTAGE));
        questionSetDetailsMap.put(Constants.SECTION_WEIGHTAGE, Double.parseDouble(hierarchySection.get(Constants.SECTION_WEIGHTAGE).toString()));
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

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, requestBody.get(Constants.USER_ID));
            propertyMap.put(Constants.ASSESSMENT_ID_KEY, requestBody.get(Constants.ASSESSMENT_ID_KEY).toString());
            propertyMap.put(Constants.CONTENT_ID_KEY, requestBody.get(Constants.CONTENT_ID_KEY).toString());
            propertyMap.put(Constants.VERSION_KEY, requestBody.get(Constants.VERSION_KEY).toString());

            List<Map<String, Object>> existingDataList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_CQF_ASSESSMENT_DATA,
                    propertyMap, null);
            if (existingDataList.isEmpty()) {
                response.put(Constants.CQF_RESULTS, new HashMap<>());
                return response;
            }
            List<Map<String, Object>> cqfResults = new ArrayList<>();
            Map<String, Object> resultMap = new HashMap<>();

            for (Map<String, Object> data : existingDataList) {
                if (!Constants.SUBMITTED.equalsIgnoreCase((String) data.get(Constants.STATUS))) {
                    Map<String, Object> dataMap = new HashMap<>();
                    dataMap.put(Constants.USER_ID, data.get(Constants.USER_ID));
                    dataMap.put(Constants.STATUS_IS_IN_PROGRESS, true);
                    cqfResults.add(dataMap);
                } else {
                    String submitResponse = (String) data.get(Constants.SUBMIT_ASSESSMENT_RESPONSE_KEY);
                    if (StringUtils.isNotBlank(submitResponse)) {
                        Map<String, Object> submitResponseMap = new HashMap<>();
                        submitResponseMap.put(Constants.USER_ID, data.get(Constants.USER_ID));
                        submitResponseMap.put(Constants.STATUS_IS_IN_PROGRESS, false);
                        submitResponseMap.putAll(objectMapper.readValue(submitResponse, new TypeReference<Map<String, Object>>() {
                        }));
                        cqfResults.add(submitResponseMap);
                    }
                }
            }
            resultMap.put(Constants.CQF_RESULTS, cqfResults);
            response.setResult(resultMap);
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


        if (!missingAttribs.isEmpty()) {
            errMsg = "One or more mandatory fields are missing in Request. Mandatory fields are : "
                    + missingAttribs.toString();
        }
        return errMsg;
    }

    /**
     * Creates a new CQF question set and adds it to the Elasticsearch service.
     *
     * @param authToken   the authentication token of the user creating the question set
     * @param requestBody the request body containing the parameters for creating the question set
     * @return the API response containing the created question set
     */
    @Override
    public SBApiResponse createCQFQuestionSet(String authToken, Map<String, Object> requestBody) {
        // Create a default response object
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CQF_API_CREATE_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(outgoingResponse, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
            return outgoingResponse;
        }
        // Create the question set and retrieve its identifier
        Map<String, Object> map = createQuestionSet(requestBody);
        map.put(Constants.IS_CQF_ASSESSMENT_ACTIVE, false);
        String identifier = map.get(Constants.IDENTIFIER).toString();
        if (identifier == null) {
            return outgoingResponse;
        }
        // Read the question set from the assessment service
        Map<String, Object> questionSetReadMap = readQuestionSet(identifier);

        // Check if the CQF assessment already exists in Elasticsearch
        Map<String, Object> esCQFAssessmentMap = getCQFAssessmentsByIds(identifier);
        boolean isCQFAssessmentExist = !ObjectUtils.isEmpty(esCQFAssessmentMap);
        // If the CQF assessment does not exist, create a new one
        if (!isCQFAssessmentExist) {
            Map<String, Object> questionSetMap = objectMapper.convertValue(questionSetReadMap.get(Constants.QUESTION_SET_LOWER_CASE), new TypeReference<Map<String, Object>>() {
            });
            questionSetMap.put(Constants.IS_CQF_ASSESSMENT_ACTIVE, false);
            esCQFAssessmentMap = questionSetMap;
        }

        // Update or add the CQF assessment to Elasticsearch
        RestStatus status = updateOrAddEntity(serverProperties.getQuestionSetHierarchyIndex(), serverConfig.getEsProfileIndexType(), identifier, esCQFAssessmentMap, isCQFAssessmentExist);
        if (status.equals(RestStatus.CREATED) || status.equals(RestStatus.OK)) {
            outgoingResponse.setResponseCode(HttpStatus.OK);
            outgoingResponse.getResult().put(Constants.IDENTIFIER, map.get(Constants.IDENTIFIER));
            outgoingResponse.getResult().put(Constants.VERSION_KEY, map.get(Constants.VERSION_KEY));
            outgoingResponse.getResult().put(Constants.IS_CQF_ASSESSMENT_ACTIVE, false);

        } else {
            outgoingResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            outgoingResponse.getParams().setErrmsg("Failed to add details to ES Service");
        }
        return outgoingResponse;
    }


    /**
     * Creates a new question set by sending a POST request to the assessment host.
     *
     * @param requestBody the request body containing the parameters for creating the question set
     * @return the created question set as a map of strings to objects
     */

    private Map<String, Object> createQuestionSet(Map<String, Object> requestBody) {
        String sbUrl = serverProperties.getAssessmentHost() + serverProperties.getQuestionSetCreate();
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
        Map<String, Object> data = outboundRequestHandlerService.fetchResultUsingPost(sbUrl, requestBody, headers);
        return objectMapper.convertValue(data.get(Constants.RESULT), new TypeReference<Map<String, Object>>() {
        });
    }

    /**
     * Reads a question set by its identifier.
     *
     * @param identifier the identifier of the question set to read
     * @return the question set as a map of strings to objects
     */
    private Map<String, Object> readQuestionSet(String identifier) {
        Map<String, Object> questionsetRead = objectMapper.convertValue(
                outboundRequestHandlerService.fetchResult(serverProperties.getAssessmentHost() + serverProperties.getQuestionSetRead() + identifier),
                new TypeReference<Map<String, Object>>() {
                });
        return objectMapper.convertValue(questionsetRead.get("result"), new TypeReference<Map<String, Object>>() {
        });
    }


    /**
     * Updates or adds an entity to the Elasticsearch index.
     *
     * @param indexName  the name of the Elasticsearch index
     * @param indexType  the type of the Elasticsearch index
     * @param identifier the identifier of the entity to update or add
     * @param data       the data to update or add
     * @param isExist    whether the entity already exists in the index
     * @return the status of the update or add operation
     */
    public RestStatus updateOrAddEntity(String indexName, String indexType, String identifier, Map<String, Object> data, boolean isExist) {
        if (isExist) {
            return indexerService.updateEntity(indexName, indexType, identifier, data);
        } else {
            return indexerService.addEntity(indexName, indexType, identifier, data);
        }
    }

    /**
     * Retrieves a CQF assessment by its registration code.
     *
     * @param assessmentIdentifier the assessmentIdentifier  of the CQF assessment to retrieve
     * @return the CQF assessment as a map of strings to objects, or an empty map if the retrieval fails
     */
    public Map<String, Object> getCQFAssessmentsByIds(String assessmentIdentifier) {
        try {
            return indexerService.readEntity(serverProperties.getQuestionSetHierarchyIndex(),
                    serverConfig.getEsProfileIndexType(), assessmentIdentifier);
        } catch (Exception e) {
            logger.error("Failed to get AssessemntId. Exception: ", e);
            logger.warn(String.format("Exception in %s : %s", "getCQFAssessmentsByIds", e.getMessage()));
        }
        return Collections.emptyMap();
    }

    /**
     * Updates a CQF question set.
     *
     * @param authToken   the authentication token for the request
     * @param requestBody the request body containing the question set data
     * @return the response from the API, including the updated question set data
     */
    @Override
    public SBApiResponse updateCQFQuestionSet(String authToken, Map<String, Object> requestBody) {
        // Create a default response object
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CQF_API_UPDATE_ASSESSMENT);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(outgoingResponse, Constants.USER_ID_DOESNT_EXIST, HttpStatus.INTERNAL_SERVER_ERROR);
            return outgoingResponse;
        }
        String sbUrl = serverProperties.getAssessmentHost() + serverProperties.getQuestionSetHierarchyUpdate();
        Map<String, String> headers = new HashMap<>();
        // Set up the authorization header with the SB API key
        headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
        // Send a PATCH request to the assessment host to update the question set hierarchy
        Map<String, Object> data = outboundRequestHandlerService.fetchResultUsingPatch(sbUrl, requestBody, headers);
        // Extract the request map from the request body
        Map<String, Object> requestMap = objectMapper.convertValue(requestBody.get(Constants.REQUEST), new TypeReference<Map<String, Object>>() {
        });
        // Extract the data map from the request map
        Map<String, Object> dataMap = objectMapper.convertValue(requestMap.get(Constants.DATA), new TypeReference<Map<String, Object>>() {
        });
        // Extract the hierarchy map from the data map
        Map<String, Object> hierarchyMap = objectMapper.convertValue(dataMap.get(Constants.HIERARCHY), new TypeReference<Map<String, Object>>() {
        });
        // Get the identifier from the hierarchy map
        String identifier = hierarchyMap.entrySet().iterator().next().getKey();
        // Get the CQF assessment data from the Elasticsearch index
        Map<String, Object> esCQFAssessmentMap = getCQFAssessmentsByIds(identifier);
        // Get the question set hierarchy data from the Elasticsearch index
        Map<String, Object> questionsetMap = readQuestionSetHierarchy(identifier);
        // Convert the question set hierarchy data to lowercase
        Map<String, Object> lowerCaseMap = questionsetMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));
        // Extract the question set data from the lowercase map
        Map<String, Object> questionSetMap = objectMapper.convertValue(lowerCaseMap.get(Constants.QUESTION_SET_LOWER_CASE), new TypeReference<Map<String, Object>>() {
        });
        // Check if the CQF assessment exists in the Elasticsearch index
        boolean isCQFAssessmentExist = !ObjectUtils.isEmpty(esCQFAssessmentMap);
        // Update or add the question set data to the Elasticsearch index
        RestStatus status = updateOrAddEntity(serverProperties.getQuestionSetHierarchyIndex(), serverConfig.getEsProfileIndexType(), identifier, questionSetMap, isCQFAssessmentExist);
        // Set the response code based on the status of the update operation
        if (status.equals(RestStatus.CREATED) || status.equals(RestStatus.OK)) {
            outgoingResponse.setResponseCode(HttpStatus.OK);
            Map<String, Object> resultMap = objectMapper.convertValue(data.get(Constants.RESULT), new TypeReference<Map<String, Object>>() {
            });
            outgoingResponse.getResult().put(Constants.IDENTIFIER, resultMap.get(Constants.IDENTIFIER));
            outgoingResponse.getResult().put(Constants.IDENTIFIERS, resultMap.get(Constants.IDENTIFIERS));
        } else {
            outgoingResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            outgoingResponse.getParams().setErrmsg("Failed to add details to ES Service");
        }
        return outgoingResponse;
    }

    /**
     * Reads the question set hierarchy from the server based on the provided identifier.
     *
     * @param identifier the identifier of the question set to read
     * @return a map of question set data
     */
    private Map<String, Object> readQuestionSetHierarchy(String identifier) {
        Map<String, Object> questionsetRead = objectMapper.convertValue(
                outboundRequestHandlerService.fetchResult(serverProperties.getAssessmentHost() + serverProperties.getQuestionSetHierarchy() + identifier + "?mode=edit"),
                new TypeReference<Map<String, Object>>() {
                });
        return objectMapper.convertValue(questionsetRead.get(Constants.RESULT), new TypeReference<Map<String, Object>>() {
        });
    }

    /**
     * Computes the result of a section based on section marks, total marks, and minimum pass value.
     *
     * @param sectionMarks the marks obtained in the section.
     * @param totalMarks the total marks available for the section.
     * @param minimumPassValue the minimum percentage required to pass the section.
     * @param resultMap the map to store the section result.
     */
    private  void computeSectionResults(Double sectionMarks, Integer totalMarks, int minimumPassValue, Map<String, Object> resultMap) {
        logger.info("Computing section results...");
        if (sectionMarks > 0 && totalMarks>0 && ((sectionMarks / totalMarks) * 100 >= minimumPassValue)) {
            resultMap.put(Constants.SECTION_RESULT, Constants.PASS);
        } else {
            resultMap.put(Constants.SECTION_RESULT, Constants.FAIL);
        }
        logger.info("Section results computed successfully.");
    }

    private Map<String, Object> calculateSectionFinalResults(List<Map<String, Object>> sectionLevelsResults) {
        double totalAchievedPercentageScore = 0.0;
        double totalOverallSectionPercentageScore = 0.0;
        double overalAssessmentScore=0.0;
        Map<String, Object> res = new HashMap<>();
        for (Map<String, Object> sectionChildren : sectionLevelsResults) {
            res.put(Constants.CHILDREN, sectionLevelsResults);
            totalAchievedPercentageScore=totalAchievedPercentageScore + (double)sectionChildren.get(Constants.ACHIEVED_PERCENTAGE_SCORE);
            totalOverallSectionPercentageScore =totalOverallSectionPercentageScore + (double) sectionChildren.get(Constants.OVERALL_SECTION_PERCENTAGE_SCORE);
        }
        if (totalOverallSectionPercentageScore > 0) {
            overalAssessmentScore = (totalAchievedPercentageScore / totalOverallSectionPercentageScore) * 100;
        }
        res.put(Constants.OVERALL_ASSESSMENT_SCORE,overalAssessmentScore);
        return res;
    }

    @Override
    public SBApiResponse readQuestionList(Map<String, Object> requestBody, String authUserToken, boolean editMode) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_QUESTIONS_LIST);
        String errMsg;
        Map<String, String> result = new HashMap<>();
        try {
            List<String> identifierList = new ArrayList<>();
            List<Object> questionList = new ArrayList<>();
            result = validateQuestionListAPI(requestBody, authUserToken, identifierList, editMode);
            errMsg = result.get(Constants.ERROR_MESSAGE);
            if (StringUtils.isNotBlank(errMsg)) {
                updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
                return response;
            }

            String assessmentIdFromRequest = (String) requestBody.get(Constants.ASSESSMENT_ID_KEY);
            Map<String, Object> questionsMap = assessUtilServ.readQListfromCache(identifierList, assessmentIdFromRequest, editMode, authUserToken);
            for (String questionId : identifierList) {
                questionList.add(assessUtilServ.filterQuestionMapDetailV2((Map<String, Object>) questionsMap.get(questionId),
                        result.get(Constants.PRIMARY_CATEGORY)));
            }
            if (errMsg.isEmpty() && identifierList.size() == questionList.size()) {
                response.getResult().put(Constants.QUESTIONS, questionList);
            }
        } catch (Exception e) {
            errMsg = String.format("Failed to fetch the question list. Exception: %s", e.getMessage());
            logger.error(errMsg, e);
        }
        if (StringUtils.isNotBlank(errMsg)) {
            updateErrorDetails(response, errMsg, HttpStatus.BAD_REQUEST);
        }
        return response;
    }

    private Map<String, String> validateQuestionListAPI(Map<String, Object> requestBody, String authUserToken,
                                                        List<String> identifierList, boolean editMode) throws IOException {
        Map<String, String> result = new HashMap<>();
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
        if (StringUtils.isBlank(userId)) {
            result.put(Constants.ERROR_MESSAGE, Constants.USER_ID_DOESNT_EXIST);
            return result;
        }
        String assessmentIdFromRequest = (String) requestBody.get(Constants.ASSESSMENT_ID_KEY);
        if (StringUtils.isBlank(assessmentIdFromRequest)) {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_ID_KEY_IS_NOT_PRESENT_IS_EMPTY);
            return result;
        }
        identifierList.addAll(getQuestionIdList(requestBody));
        if (identifierList.isEmpty()) {
            result.put(Constants.ERROR_MESSAGE, Constants.IDENTIFIER_LIST_IS_EMPTY);
            return result;
        }

        Map<String, Object> assessmentAllDetail = assessUtilServ
                .readAssessmentHierarchyFromCache(assessmentIdFromRequest, editMode, authUserToken);

        if (MapUtils.isEmpty(assessmentAllDetail)) {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_HIERARCHY_READ_FAILED);
            return result;
        }
        String primaryCategory = (String) assessmentAllDetail.get(Constants.PRIMARY_CATEGORY);
        if (Constants.PRACTICE_QUESTION_SET
                .equalsIgnoreCase(primaryCategory) || editMode) {
            result.put(Constants.PRIMARY_CATEGORY, primaryCategory);
            result.put(Constants.ERROR_MESSAGE, StringUtils.EMPTY);
            return result;
        }

        Map<String, Object> userAssessmentAllDetail = new HashMap<>();
        List<Map<String, Object>> existingDataList = readUserSubmittedAssessmentRecords(new CQFAssessmentModel(userId, requestBody.get(Constants.ASSESSMENT_ID_KEY).toString(), requestBody.get(Constants.CONTENT_ID_KEY).toString(), requestBody.get(Constants.VERSION_KEY).toString()));
        String questionSetFromAssessmentString = (!existingDataList.isEmpty())
                ? (String) existingDataList.get(0).get(Constants.ASSESSMENT_READ_RESPONSE_KEY)
                : "";
        if (StringUtils.isNotBlank(questionSetFromAssessmentString)) {
            userAssessmentAllDetail.putAll(objectMapper.readValue(questionSetFromAssessmentString,
                    new TypeReference<Map<String, Object>>() {
                    }));
        } else {
            result.put(Constants.ERROR_MESSAGE, Constants.USER_ASSESSMENT_DATA_NOT_PRESENT);
            return result;
        }

        if (!MapUtils.isEmpty(userAssessmentAllDetail)) {
            result.put(Constants.PRIMARY_CATEGORY, (String) userAssessmentAllDetail.get(Constants.PRIMARY_CATEGORY));
            List<String> questionsFromAssessment = new ArrayList<>();
            List<Map<String, Object>> sections = (List<Map<String, Object>>) userAssessmentAllDetail
                    .get(Constants.CHILDREN);
            for (Map<String, Object> section : sections) {
                // Out of the list of questions received in the payload, checking if the request
                // has only those ids which are a part of the user's latest assessment
                // Fetching all the remaining questions details from the Redis
                questionsFromAssessment.addAll((List<String>) section.get(Constants.CHILD_NODES));
            }
            if (validateQuestionListRequest(identifierList, questionsFromAssessment)) {
                result.put(Constants.ERROR_MESSAGE, StringUtils.EMPTY);
            } else {
                result.put(Constants.ERROR_MESSAGE, Constants.THE_QUESTIONS_IDS_PROVIDED_DONT_MATCH);
            }
            return result;
        } else {
            result.put(Constants.ERROR_MESSAGE, Constants.ASSESSMENT_ID_INVALID);
            return result;
        }
    }

    private Boolean validateQuestionListRequest(List<String> identifierList, List<String> questionsFromAssessment) {
        return (new HashSet<>(questionsFromAssessment).containsAll(identifierList)) ? Boolean.TRUE : Boolean.FALSE;
    }

    private List<String> getQuestionIdList(Map<String, Object> questionListRequest) {
        try {
            if (questionListRequest.containsKey(Constants.REQUEST)) {
                Map<String, Object> request = (Map<String, Object>) questionListRequest.get(Constants.REQUEST);
                if ((!ObjectUtils.isEmpty(request)) && request.containsKey(Constants.SEARCH)) {
                    Map<String, Object> searchObj = (Map<String, Object>) request.get(Constants.SEARCH);
                    if (!ObjectUtils.isEmpty(searchObj) && searchObj.containsKey(Constants.IDENTIFIER)
                            && !org.apache.commons.collections.CollectionUtils.isEmpty((List<String>) searchObj.get(Constants.IDENTIFIER))) {
                        return (List<String>) searchObj.get(Constants.IDENTIFIER);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(String.format("Failed to process the questionList request body. %s", e.getMessage()));
        }
        return Collections.emptyList();
    }

    /**
     * Process CQF post-publish event by updating the question set hierarchy in the Elasticsearch index.
     *
     * @param assessmentId the ID of the assessment
     */
    @Override
    public void processCQFPostPublish(String assessmentId) {
        logger.info("Inside the processCQFPostPublish method of CQFAssessmentServiceImpl");
        // Get the CQF assessment data from the Elasticsearch index
        Map<String, Object> esCQFAssessmentMap = getCQFAssessmentsByIds(assessmentId);
        // Get the question set hierarchy data from the Elasticsearch index
        Map<String, Object> questionsetMap = readQuestionSetHierarchy(assessmentId);
        // Convert the question set hierarchy data to lowercase
        Map<String, Object> lowerCaseMap = questionsetMap.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().toLowerCase(), Map.Entry::getValue));
        // Extract the question set data from the lowercase map
        Map<String, Object> questionSetMap = objectMapper.convertValue(lowerCaseMap.get(Constants.QUESTION_SET_LOWER_CASE), new TypeReference<Map<String, Object>>() {
        });
        // Check if the CQF assessment exists in the Elasticsearch index
        boolean isCQFAssessmentExist = !ObjectUtils.isEmpty(esCQFAssessmentMap);
        // Get the 'children' ArrayList
        ArrayList<Object> children = (ArrayList<Object>) questionSetMap.get(Constants.CHILDREN);
        // Iterate over the 'children' ArrayList
        for (Object child : children) {
            // Get the 'children' ArrayList of the current child
            ArrayList<Object> childChildren = (ArrayList<Object>) ((HashMap) child).get(Constants.CHILDREN);
            // Iterate over the 'children' ArrayList of the current child
            for (Object grandChild : childChildren) {
                // Get the 'choices' string
                String choices = (String) ((HashMap) grandChild).get(Constants.CHOICES);
                // Convert the 'choices' string to a HashMap using ObjectMapper
                try {
                    HashMap<String, Object> choicesMap = objectMapper.convertValue(objectMapper.readTree(choices), HashMap.class);
                    // Replace the 'choices' string with the HashMap
                    ((HashMap) grandChild).put(Constants.CHOICES, choicesMap);
                } catch (Exception e) {
                    // Log the error message
                    logger.error("Error converting choices to HashMap: {}", e.getMessage());
                }

            }
        }
        questionSetMap.put(Constants.CHILDREN, children);
        // Update or add the question set data to the Elasticsearch index
        RestStatus status = updateOrAddEntity(serverProperties.getQuestionSetHierarchyIndex(), serverConfig.getEsProfileIndexType(), assessmentId, questionSetMap, isCQFAssessmentExist);
        if (status.equals(RestStatus.CREATED) || status.equals(RestStatus.OK)) {
            logger.info("Updated the question set hierarchy in the Elasticsearch index successfully.");
        } else {
            logger.info("There is a issue while updating the question set hierarchy in the Elasticsearch index.");
        }
    }


    /**
     * Processes the 'Not Applicable' options in the question map and updates the indexValueNotApplicableForQnsMap map.
     *
     * @param questionMap                   the question map to process
     * @param indexValueNotApplicableForQnsMap the map to store the index of 'Not Applicable' options
     */
    private void processNotApplicableValueInOptions(Map<String, Object> questionMap, Map<String, Integer> indexValueNotApplicableForQnsMap) {
        logger.info("Processing 'Not Applicable' options in the question map.");
        for (Map.Entry<String, Object> entry : questionMap.entrySet()) {
            Map<String, Object> questionsMap = (Map<String, Object>) entry.getValue();
            if (questionsMap.containsKey(Constants.CHOICES)) {
                Map<String, Object> choicesMap = (Map<String, Object>) questionsMap.get(Constants.CHOICES);
                List<Map<String, Object>> optionsList = (List<Map<String, Object>>) choicesMap.get(Constants.OPTIONS);
                findNotApplicableValueOption(optionsList, entry.getKey(), indexValueNotApplicableForQnsMap);
            }
        }
        logger.info("Finished processing 'Not Applicable' options in the question map.");
    }

    /**
     * Finds the index of the "Not Applicable" option in the given options list and updates the indexNotApplicableForQuestion map.
     *
     * @param optionsList                   the list of options to search
     * @param questionKey                   the key of the question being processed
     * @param indexNotApplicableForQuestion the map to store the index of "Not Applicable" options
     */
    private void findNotApplicableValueOption(List<Map<String, Object>> optionsList, String questionKey, Map<String, Integer> indexNotApplicableForQuestion) {
        for (int i = 0; i < optionsList.size(); i++) {
            Map<String, Object> optionMap = optionsList.get(i);
            Map<String, Object> valueMap = (Map<String, Object>) optionMap.get(Constants.VALUE);
            String body = (String) valueMap.get(Constants.BODY);
            if (body.contains(Constants.NOT_APPLICABLE)) {
                indexNotApplicableForQuestion.put(questionKey, i);
                break;
            }
        }
    }
}