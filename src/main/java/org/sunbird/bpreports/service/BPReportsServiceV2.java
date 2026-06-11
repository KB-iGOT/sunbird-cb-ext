package org.sunbird.bpreports.service;

import org.sunbird.common.model.SBApiResponse;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;

import java.util.Map;

/**
 * Service contract for Blended Program V2 report generation.
 */
public interface BPReportsServiceV2 {

    /**
     * Validates the request, checks for an existing IN-PROGRESS report, and publishes
     * a Kafka event with version="v2" to trigger async report generation.
     *
     * @param requestBody the API request body containing a nested {@code request} map
     *                    with courseId, batchId, orgId, and reportRequester
     * @param authToken   bearer token used to resolve and validate the calling user
     * @return {@link SBApiResponse} with status IN-PROGRESS if already running,
     *         SUCCESS if the event was published, or an error response on validation failure
     */
    SBApiResponse generateBPReportV2(Map<String, Object> requestBody, String authToken);

    ResponseEntity<Resource> downloadBPReportV2(String authToken, String orgId, String courseId, String batchId, String fileName);

    /**
     * Kafka consumer entry point for V2 report generation. Fetches batch metadata,
     * enrolled users, user profiles, and certificate statuses, then builds and uploads
     * a 46-column SXSSF Excel report to cloud storage.
     *
     * @param request deserialized Kafka message payload containing courseId, batchId,
     *                orgId, reportRequester, and optional surveyId
     */
    void processBPReportV2(Map<String, Object> request);
}
