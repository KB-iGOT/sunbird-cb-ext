package org.sunbird.cqfassessment.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

/**
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
     * @param authToken the authentication token for the request
     * @return the API response containing the list of assessments
     */
    SBApiResponse listCQFAssessments(String authToken);

    SBApiResponse readAssessment(String assessmentIdentifier, String token, boolean edit);
}