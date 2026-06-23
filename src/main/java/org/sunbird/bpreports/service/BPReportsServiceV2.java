package org.sunbird.bpreports.service;

import org.sunbird.common.model.SBApiResponse;

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

    /**
     * Kafka consumer entry point for V2 report generation. Fetches batch metadata,
     * enrolled users, user profiles, and certificate statuses, then builds and uploads
     * a 46-column SXSSF Excel report to cloud storage.
     *
     * @param request deserialized Kafka message payload containing courseId, batchId,
     *                orgId, reportRequester, and optional surveyId
     */
    void processBPReportV2(Map<String, Object> request);

    /**
     * Returns the current status of a V2 BP report for a given batch and requester.
     * Queries bp_enrolment_report_v2 using orgId, courseId, batchId, reportRequester,
     * and contextType as composite key — ensuring V2 records are never mixed with V1.
     *
     * @param requestBody the API request body containing a nested {@code request} map
     *                    with orgId, courseId, batchId, and reportRequester
     * @param authToken   bearer token used to resolve and validate the calling user
     * @return {@link SBApiResponse} with report status, downloadLink, and enrolment counts
     *         if found, or an informational error if no report exists for the given key
     */
    SBApiResponse getBPReportStatusV2(Map<String, Object> requestBody, String authToken);

    /**
     * Returns a combined list of BP reports from both V1 (bp_enrolment_report) and
     * V2 (bp_enrolment_report_v2) tables for the given orgId, courseId, batchId,
     * and reportRequester. Response structure is identical to the individual status APIs.
     *
     * @param requestBody the API request body containing a nested {@code request} map
     *                    with orgId, courseId, batchId, and reportRequester
     * @param authToken   bearer token used to resolve and validate the calling user
     * @return {@link SBApiResponse} with combined content list and total count
     */
    SBApiResponse getBPReportList(Map<String, Object> requestBody, String authToken);
}
