package org.sunbird.cqfassessment.service;

import org.sunbird.common.model.SBApiResponse;

import javax.validation.Valid;
import java.util.Map;

/**
 * @author mahesh.vakkund
 * Service interface for managing CQF Assessments.
 */
public interface CQFAssessmentService {

    /**
     * Creates a entry for new CQF Assessment.
     *
     * @param authToken the authentication token for the request
     * @param requestBody the request body containing the assessmentId and the status
     * @return the API response containing the created assessment details
     */
    SBApiResponse createCQFAssessment(String authToken, Map<String, Object> requestBody);

    /**
     * Updates an existing CQF Assessment.
     *
     * @param requestBody the request body containing the updated assessment status
     * @param authToken the authentication token for the request
     * @param cqfAssessmentIdentifier the identifier of the assessment to update
     * @return the API response containing the updated assessment details
     */
    SBApiResponse updateCQFAssessment(Map<String, Object> requestBody, String authToken, String cqfAssessmentIdentifier);

    /**
     * Retrieves a CQF Assessment by its identifier.
     *
     * @param authToken the authentication token for the request
     * @param cqfAssessmentIdentifier the identifier of the assessment to retrieve
     * @return the API response containing the assessment details
     */
    SBApiResponse getCQFAssessment(String authToken, String cqfAssessmentIdentifier);

    /**
     * Lists all CQF Assessments.
     *
     * @param authToken   the authentication token for the request
     * @param requestBody  Request body of the request
     * @return the API response containing the list of assessments
     */
    SBApiResponse listCQFAssessments(String authToken, @Valid Map<String, Object> requestBody);

    /**
     * Reads an assessment from the API.
     *
     * @param assessmentIdentifier Unique identifier of the assessment to read.
     * @param token                Authentication token for the API request.
     * @param edit                 Whether the assessment should be retrieved in edit mode.
     * @param contentId            Identifier of the content associated with the assessment.
     * @param versionKey           Version key of the assessment.
     * @return SBApiResponse containing the assessment data.
     */
    SBApiResponse readAssessment(String assessmentIdentifier, String token, boolean edit, String contentId, String versionKey);

    /**
     * Submits a CQF Assessment.
     * <p>
     * This method is used to submit a CQF Assessment with the provided request body and authentication token.
     *
     * @param requestBody   The request body containing the assessment data to be submitted.
     * @param authUserToken The authentication token of the user submitting the assessment.
     * @param edit          Whether the assessment is being submitted in edit mode.
     * @return The API response containing the result of the submission.
     */
    SBApiResponse submitCQFAssessment(@Valid Map<String, Object> requestBody, String authUserToken, boolean edit);

    /**
     * Retrieves the result of a CQF assessment.
     *
     * @param requestBody   the request body containing the assessment identifier and other parameters
     * @param authUserToken the authentication token of the user requesting the assessment result
     * @return the API response containing the assessment result
     */
    SBApiResponse readCQFAssessmentResult(@Valid Map<String, Object> requestBody, String authUserToken);


    /**
     * Creates a new CQF question set.
     *
     * @param authToken   the authentication token of the user creating the question set
     * @param requestBody the request body containing the parameters for creating the question set
     * @return the API response containing the created question set
     */
    SBApiResponse createCQFQuestionSet(String authToken, @Valid Map<String, Object> requestBody);

    /**
     * Updates an existing CQF question set.
     *
     * @param authToken   the authentication token of the user updating the question set
     * @param requestBody the request body containing the updated parameters for the question set
     * @return the API response containing the updated question set
     */
    SBApiResponse updateCQFQuestionSet(String authToken, @Valid Map<String, Object> requestBody);

    SBApiResponse readQuestionList(@Valid Map<String, Object> requestBody, String authUserToken, boolean edit);


    /**
     * Process CQF post-publish event by updating the question set hierarchy in the Elasticsearch index.
     *
     * @param assessmentId the ID of the assessment
     */
    void processCQFPostPublish(String assessmentId);

}