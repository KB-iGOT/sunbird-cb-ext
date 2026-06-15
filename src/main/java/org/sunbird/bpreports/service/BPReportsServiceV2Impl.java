package org.sunbird.bpreports.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.bpreports.postgres.entity.WfStatusEntity;
import org.sunbird.bpreports.postgres.repository.WfStatusEntityRepository;
import org.sunbird.cassandra.utils.CassandraOperationImpl;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.storage.service.StorageService;
import org.sunbird.user.service.UserUtilityService;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles /bp/v2/generate/report — Blended Program report generation.
 * <p>
 * Validates the request, writes an IN_PROGRESS record to bp_enrolment_report,
 * and publishes a Kafka event with version: "v2" so the consumer routes to the
 * V2Handler for blended-specific data enrichment (sessions, assessments, certificates).
 * <p>
 * Shares the same Cassandra table and Kafka topic as BPReportsServiceImpl (v1).
 * The version field in the Kafka event is the only differentiator downstream.
 */
@RequiredArgsConstructor
@Service
public class BPReportsServiceV2Impl implements BPReportsServiceV2 {

    private static final Logger logger = LoggerFactory.getLogger(BPReportsServiceV2Impl.class);

    private final AccessTokenValidator accessTokenValidator;
    private final UserUtilityService userUtilityService;
    private final CassandraOperationImpl cassandraOperation;
    private final Producer kafkaProducer;
    private final CbExtServerProperties serverProperties;
    private final WfStatusEntityRepository wfStatusEntityRepository;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final ObjectMapper objectMapper;
    private final StorageService storageService;

    /**
     * Entry point for /bp/v2/generate/report. Returns 200 immediately —
     * actual report generation is async via Kafka consumer.
     * Returns null on success; non-null means validation failed.
     */
    @Override
    public SBApiResponse generateBPReportV2(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API_V2);
        try {
            String userId = validateUserToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                return response;
            }
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            SBApiResponse validationError = validateGenerateReportRequestBody(request);
            if (!ObjectUtils.isEmpty(validationError)) {
                return validationError;
            }
            String courseId = (String) request.get(Constants.COURSE_ID);
            String batchId = (String) request.get(Constants.BATCH_ID);
            String orgId = (String) request.get(Constants.ORG_ID);
            if (!validateUserOrganization(userId, orgId, response)) {
                return response;
            }
            return handleReportGeneration(userId, request, courseId, batchId, orgId);
        } catch (Exception e) {
            logger.error(Constants.ERROR_PROCESSING_BP_REQUEST, e);
            updateErrorDetails(response, Constants.ERROR_PROCESSING_BP_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    /**
     * Mutates response in-place with the error detail before returning null on failure,
     * so the caller can return response immediately without building a separate error object.
     */
    private String validateUserToken(String authToken, SBApiResponse response) {
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            updateErrorDetails(response, "Invalid user ID from auth token.", HttpStatus.BAD_REQUEST);
            return null;
        }
        return userId;
    }

    /**
     * OrgId comparison is case-sensitive and must match the user's rootOrgId exactly —
     * prevents an admin of org-A from triggering reports scoped to org-B.
     */
    private boolean validateUserOrganization(String userId, String orgId, SBApiResponse response) {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        userUtilityService.getUserDetailsFromDB(
                Collections.singletonList(userId),
                Arrays.asList(Constants.USER_ID,Constants.ROOT_ORG_ID),
                userInfoMap);
        String userOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
        if (StringUtils.isBlank(userOrgId) || !userOrgId.equals(orgId)) {
            updateErrorDetails(response, "Requested User is not belongs to same organisation.", HttpStatus.BAD_REQUEST);
            return false;
        }
        return true;
    }

    /**
     * Idempotency guard: if a report is already IN_PROGRESS for this
     * (orgId, courseId, batchId, reportRequester) combination, returns IN_PROGRESS
     * without publishing a new Kafka event — prevents duplicate report generation
     * when the API is called concurrently or retried.
     * FAILED and COMPLETED records are re-triggered as a fresh run.
     */
    private SBApiResponse handleReportGeneration(String userId, Map<String, Object> request,
                                                 String courseId, String batchId, String orgId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        List<Map<String, Object>> existingReports = findExistingReport(courseId, batchId, orgId, request);
        if (CollectionUtils.isEmpty(existingReports)) {
            logger.info("Insert BP report details into DB::started");
            return insertReportDetailsInDBAndTriggerKafkaEvent(userId, request);
        }
        String reportStatus = (String) existingReports.get(0).get(Constants.STATUS);
        if (Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase(reportStatus)) {
            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            response.setResponseCode(HttpStatus.OK);
            return response;
        }
        logger.info("Update BP report details::started");
        return updateReportDetailsInDBAndTriggerKafkaEvent(userId, request);
    }

    /**
     * One record per (orgId, courseId, batchId, reportRequester) — each requester role
     * (MDO_ADMIN, MDO_LEADER, PROGRAM_COORDINATOR) gets its own report row for the same batch.
     */
    private List<Map<String, Object>> findExistingReport(String courseId, String batchId,
                                                         String orgId, Map<String, Object> request) {
        Map<String, Object> searchCriteria = new HashMap<>();
        searchCriteria.put(Constants.ORG_ID, orgId);
        searchCriteria.put(Constants.COURSE_ID, courseId);
        searchCriteria.put(Constants.BATCH_ID, batchId);
        searchCriteria.put(Constants.REPORT_REQUESTER, request.get(Constants.REPORT_REQUESTER));
        searchCriteria.put(Constants.CONTEXT_TYPE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
        return cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD,
                Constants.BP_ENROLMENT_REPORT_TABLE_V2,
                searchCriteria,
                Collections.singletonList(Constants.STATUS));
    }

    /**
     * Returns null on success — caller checks via ObjectUtils.isEmpty().
     * reportRequester is validated against BP_REPORT_REQUESTER_ROLES allowlist
     * because each role gets a different column set in the generated report.
     */
    private SBApiResponse validateGenerateReportRequestBody(Map<String, Object> requestBody) {
        if (CollectionUtils.isEmpty(requestBody)) {
            return createErrorResponse(Constants.INVALID_REQUEST);
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.COURSE_ID))) {
            return createErrorResponse(Constants.COURSE_ID_MISSING);
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.BATCH_ID))) {
            return createErrorResponse(Constants.BATCH_ID_MISSING);
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.ORG_ID))) {
            return createErrorResponse(Constants.ORG_ID_KEY_MISSING);
        }
        String reportRequester = (String) requestBody.get(Constants.REPORT_REQUESTER);
        if (StringUtils.isEmpty(reportRequester)) {
            return createErrorResponse(Constants.REPORT_REQUESTER_MISSING);
        }
        if (!Constants.BP_REPORT_REQUESTER_ROLES.contains(reportRequester)) {
            return createErrorResponse(Constants.REPORT_REQUESTER_ERR_MSG);
        }
        return null;
    }

    /**
     * All validation failures are 400; status code is set by caller context.
     */
    private SBApiResponse createErrorResponse(String errorMessage) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        updateErrorDetails(response, errorMessage, HttpStatus.BAD_REQUEST);
        return response;
    }

    /**
     * Shared mutation used by both 4xx validation failures and 5xx internal errors.
     */
    private void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

    /**
     * DB insert happens before Kafka publish intentionally — ensures a record exists
     * before the consumer could theoretically start processing.
     * If Kafka publish throws after a successful insert, the catch block marks the
     * record as FAILED so the client can re-trigger without being stuck in IN_PROGRESS.
     */
    private SBApiResponse insertReportDetailsInDBAndTriggerKafkaEvent(String userId, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        String orgId = (String) requestBody.get(Constants.ORG_ID);
        String courseId = (String) requestBody.get(Constants.COURSE_ID);
        String batchId = (String) requestBody.get(Constants.BATCH_ID);
        String reportRequester = (String) requestBody.get(Constants.REPORT_REQUESTER);
        try {
            Map<String, Object> dbRequest = buildReportRequest(orgId, courseId, batchId, reportRequester, userId, requestBody);
            SBApiResponse dbResponse = cassandraOperation.insertRecord(
                    Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.BP_ENROLMENT_REPORT_TABLE_V2,
                    dbRequest);
            if (!dbResponse.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                logger.error(Constants.ERROR_INSERTING_BP_RECORD);
                updateErrorDetails(response, Constants.ERROR_PROCESSING_BP_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
                // Mark FAILED so the client can re-trigger rather than being stuck with no visible record
                updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
                return response;
            }
            publishKafkaEvent(orgId, courseId, batchId, reportRequester, userId, requestBody);
            response.getResult().put(Constants.STATUS, Constants.SUCCESS);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error(Constants.ERROR_INSERTING_BP_RECORD, e);
            updateErrorDetails(response, Constants.ERROR_PROCESSING_BP_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
            return response;
        }
        logger.info("Insert BP report details into DB::completed");
        return response;
    }

    /**
     * Status is reset to IN_PROGRESS before publishing to Kafka so the consumer
     * always finds a consistent state even if it processes the event before this
     * method returns. On Kafka failure, explicitly marks FAILED so the client
     * can re-trigger without being stuck in IN_PROGRESS.
     */
    private SBApiResponse updateReportDetailsInDBAndTriggerKafkaEvent(String userId, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        String orgId = (String) requestBody.get(Constants.ORG_ID);
        String courseId = (String) requestBody.get(Constants.COURSE_ID);
        String batchId = (String) requestBody.get(Constants.BATCH_ID);
        String reportRequester = String.valueOf(requestBody.get(Constants.REPORT_REQUESTER));
        try {
            boolean updateSuccess = updateReportStatus(orgId, courseId, batchId, reportRequester,
                    Constants.STATUS_IN_PROGRESS_UPPERCASE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
            if (!updateSuccess) {
                logger.error(Constants.ERROR_UPDATING_BP_RECORD);
                updateErrorDetails(response, Constants.ERROR_PROCESSING_BP_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            publishKafkaEvent(orgId, courseId, batchId, reportRequester, userId, requestBody);
            response.getResult().put(Constants.STATUS, Constants.SUCCESS);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error(Constants.ERROR_UPDATING_BP_RECORD, e);
            updateErrorDetails(response, Constants.ERROR_PROCESSING_BP_REQUEST, HttpStatus.INTERNAL_SERVER_ERROR);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
            return response;
        }
        logger.info("Updating BP report details::completed");
        return response;
    }

    /**
     * surveyId is optional — only written to DB when present, so existing rows without
     * a survey are not polluted with a null surveyId column.
     * version: "v2" must be added here so the status API reflects which endpoint triggered this report.
     */
    private Map<String, Object> buildReportRequest(String orgId, String courseId, String batchId,
                                                   String reportRequester, String userId,
                                                   Map<String, Object> requestBody) {
        Map<String, Object> dbRequest = new HashMap<>();
        dbRequest.put(Constants.ORG_ID, orgId);
        dbRequest.put(Constants.COURSE_ID, courseId);
        dbRequest.put(Constants.BATCH_ID, batchId);
        dbRequest.put(Constants.REPORT_REQUESTER, reportRequester);
        dbRequest.put(Constants.CONTEXT_TYPE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
        dbRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
        dbRequest.put(Constants.CREATED_BY, userId);
        String surveyId = (String) requestBody.get(Constants.SURVEY_ID);
        if (StringUtils.isNotEmpty(surveyId)) {
            dbRequest.put(Constants.SURVEY_ID, surveyId);
        }
        return dbRequest;
    }

    /**
     * Publishes a flat-map event to the shared bp-report Kafka topic.
     * version: "v2" is set here so the consumer routes to V2Handler
     * for blended-specific enrichment instead of the standard V1 flow.
     */
    private void publishKafkaEvent(String orgId, String courseId, String batchId,
                                   String reportRequester, String userId,
                                   Map<String, Object> requestBody) {
        Map<String, Object> kafkaRequest = new HashMap<>();
        kafkaRequest.put(Constants.ORG_ID, orgId);
        kafkaRequest.put(Constants.COURSE_ID, courseId);
        kafkaRequest.put(Constants.BATCH_ID, batchId);
        kafkaRequest.put(Constants.REPORT_REQUESTER, reportRequester);
        kafkaRequest.put(Constants.CONTEXT_TYPE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
        kafkaRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
        kafkaRequest.put(Constants.CREATED_BY, userId);
        kafkaRequest.put(Constants.BP_REPORT_VERSION, Constants.BP_REPORT_VERSION_V2);
        String surveyId = (String) requestBody.get(Constants.SURVEY_ID);
        if (StringUtils.isNotEmpty(surveyId)) {
            kafkaRequest.put(Constants.SURVEY_ID, surveyId);
        }
        kafkaProducer.push(serverProperties.getKafkaTopicBPReport(), kafkaRequest);
    }

    /**
     * Resets downloadLink, fileName, and user counts to null/0 on every status transition —
     * prevents the status API from returning a stale download link while a new report is generating.
     * lastReportGeneratedOn is stamped immediately only for FAILED; for IN_PROGRESS it is
     * cleared and set by the consumer once the report is successfully uploaded.
     */
    private boolean updateReportStatus(String orgId, String courseId, String batchId,
                                       String reportRequester, String status, String contextType) {
        Map<String, Object> compositeKey = buildCompositeKey(orgId, courseId, batchId, reportRequester, contextType);
        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put(Constants.STATUS, status);
        updateAttributes.put(Constants.DOWNLOAD_LINK, null);
        updateAttributes.put(Constants.FILE_NAME, null);
        updateAttributes.put(Constants.PENDING_USER, 0);
        updateAttributes.put(Constants.APPROVED_USER, 0);
        updateAttributes.put(Constants.REJECTED_USER, 0);
        updateAttributes.put(Constants.LAST_REPORT_GENERATED_ON,
                status.equals(Constants.FAILED_UPPERCASE) ? new Date() : null);
        Map<String, Object> response = cassandraOperation.updateRecord(
                Constants.SUNBIRD_KEY_SPACE_NAME,
                Constants.BP_ENROLMENT_REPORT_TABLE_V2,
                updateAttributes,
                compositeKey);
        return response.get(Constants.RESPONSE).equals(Constants.SUCCESS);
    }

    /**
     * All four fields are required — they form the composite partition key in
     * bp_enrolment_report and must be present in every CQL read/write operation.
     */
    private Map<String, Object> buildCompositeKey(String orgId, String courseId,
                                                  String batchId, String reportRequester,
                                                  String contextType) {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.ORG_ID, orgId);
        compositeKey.put(Constants.COURSE_ID, courseId);
        compositeKey.put(Constants.BATCH_ID, batchId);
        compositeKey.put(Constants.REPORT_REQUESTER, reportRequester);
        compositeKey.put(Constants.CONTEXT_TYPE, contextType);
        return compositeKey;
    }


    /**
     * Kafka consumer entry point — orchestrates the full V2 report pipeline for one batch.
     * Each stage (fetch, enrich, build, upload) is called sequentially; any IOException
     * from the workbook write or close triggers a FAILED status so clients can re-trigger.
     */
    @Override
    public void processBPReportV2(Map<String, Object> request) {
        String courseId = (String) request.get(Constants.COURSE_ID);
        String batchId = (String) request.get(Constants.BATCH_ID);
        String orgId = (String) request.get(Constants.ORG_ID);
        String reportRequester = (String) request.get(Constants.REPORT_REQUESTER);
        String contextType = (String) request.getOrDefault(Constants.CONTEXT_TYPE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
        logger.info("BPReportsServiceV2Impl:: processBPReportV2: Started for courseId: {}, batchId: {}", courseId, batchId);
        Map<String, Object> batchDetails = fetchBatchDetails(courseId, batchId);
        if (batchDetails.isEmpty()) {
            logger.error("BPReportsServiceV2Impl:: processBPReportV2: No batch details found for courseId: {}, batchId: {}", courseId, batchId);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, contextType);
            return;
        }
        logger.debug("BPReportsServiceV2Impl:: processBPReportV2: Batch details fetched for batchId: {}", batchId);

        List<WfStatusEntity> enrolledUsers = fetchEnrolledUsers(batchId);
        if (enrolledUsers.isEmpty()) {
            logger.warn("BPReportsServiceV2Impl:: processBPReportV2: No active enrolled users found for batchId: {}", batchId);
        }
        logger.info("BPReportsServiceV2Impl:: processBPReportV2: {} active enrolled users fetched for batchId: {}", enrolledUsers.size(), batchId);

        Map<String, Object> referenceData = fetchBatchReferenceData(batchDetails);
        logger.debug("BPReportsServiceV2Impl:: processBPReportV2: Reference data fetched for batchId: {}", batchId);

        List<String> userIds = enrolledUsers.stream()
                .map(WfStatusEntity::getUserId)
                .collect(Collectors.toList());
        Map<String, Object> allUserProfiles = batchFetchUserProfiles(userIds);
        Map<String, List<Map<String, Object>>> allCertStatuses = batchFetchCertificateStatuses(userIds, courseId, batchId);
        logger.debug("BPReportsServiceV2Impl:: processBPReportV2: User profiles and certificate statuses fetched for batchId: {}", batchId);

        List<String> createdFor = (List<String>) batchDetails.get(Constants.CREATED_FOR);
        String contentOrgId = CollectionUtils.isEmpty(createdFor) ? null : createdFor.get(0);

        List<WfStatusEntity> filteredUsers = new ArrayList<>();
        List<Map<String, Object>> userDataList = new ArrayList<>(enrolledUsers.size());
        for (WfStatusEntity entity : enrolledUsers) {
            if (StringUtils.isNotBlank(contentOrgId) && !orgId.equalsIgnoreCase(contentOrgId)) {
                Map<String, Object> userProfile = (Map<String, Object>) allUserProfiles.get(entity.getUserId());
                String userOrgId = MapUtils.isNotEmpty(userProfile) ? (String) userProfile.get(Constants.ROOT_ORG_ID) : null;
                if (!orgId.equalsIgnoreCase(userOrgId)) {
                    continue;
                }
            }
            filteredUsers.add(entity);
            userDataList.add(buildUserData(entity, allUserProfiles, allCertStatuses));
        }
        logger.info("BPReportsServiceV2Impl:: processBPReportV2: User data assembled for {} users, batchId: {}", userDataList.size(), batchId);

        int[] counts = computeEnrolmentCounts(filteredUsers);
        SXSSFWorkbook workbook = new SXSSFWorkbook(serverProperties.getBpReportExcelRowWindowSize());
        try {
            buildExcelSheet(workbook, batchDetails, referenceData, filteredUsers.size(), userDataList);
            logger.debug("BPReportsServiceV2Impl:: processBPReportV2: Excel sheet built with {} data rows for batchId: {}", userDataList.size(), batchId);
            uploadReportAndUpdateDatabase(workbook, orgId, courseId, batchId, reportRequester, counts, contextType);
        } catch (Exception e) {
            logger.error("BPReportsServiceV2Impl:: processBPReportV2: Report generation failed for batchId: {}", batchId, e);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, contextType);
        } finally {
            workbook.dispose();
            try {
                workbook.close();
            } catch (IOException e) {
                logger.warn("BPReportsServiceV2Impl:: processBPReportV2: Failed to close workbook for batchId: {}", batchId, e);
            }
        }
    }

    /**
     * Fetches active enrolled users for a batch from PostgreSQL wf_status table.
     * WITHDRAWN users are excluded at fetch time — they are never part of the report
     * and excluding them here avoids unnecessary per-user processing in later steps.
     * Paginated at 100 per page to avoid OOM on large batches.
     */
    private List<WfStatusEntity> fetchEnrolledUsers(String batchId) {
        int pageNumber = 0;
        int pageSize = serverProperties.getBpReportWfPageSize();
        Page<WfStatusEntity> page = wfStatusEntityRepository.findByApplicationId(batchId, PageRequest.of(pageNumber, pageSize));
        List<WfStatusEntity> result = new ArrayList<>((int) page.getTotalElements());
        do {
            page.getContent().stream()
                    .filter(e -> !Constants.WITHDRAWN.equalsIgnoreCase(e.getCurrentStatus()))
                    .forEach(result::add);
            pageNumber++;
            if (page.hasNext()) {
                page = wfStatusEntityRepository.findByApplicationId(batchId, PageRequest.of(pageNumber, pageSize));
            }
        } while (page.hasNext());
        return result;
    }

    /**
     * Aggregates batch-level reference data from three external sources into one map.
     * These calls happen once per report run (not per user), so the cost is fixed regardless of batch size.
     */
    private Map<String, Object> fetchBatchReferenceData(Map<String, Object> batchDetails) {
        Map<String, Object> referenceData = new HashMap<>();
        String courseId = (String) batchDetails.get(Constants.COURSE_ID);
        List<String> createdFor = (List<String>) batchDetails.get(Constants.CREATED_FOR);
        String createdBy = (String) batchDetails.get(Constants.CREATED_BY);
        fetchProgramDetails(courseId, referenceData);
        if (!CollectionUtils.isEmpty(createdFor)) {
            fetchMdoName(createdFor.get(0), referenceData);
        }
        if (StringUtils.isNotBlank(createdBy)) {
            fetchCoordinatorName(createdBy, referenceData);
        }
        fetchInstructorName(batchDetails, referenceData);
        return referenceData;
    }

    /**
     * Fetches Program Name and Program Type from the Content Read API.
     * primaryCategory is the platform field that maps to "Program Type" in the report.
     */
    private void fetchProgramDetails(String courseId, Map<String, Object> referenceData) {
        String url = serverProperties.getContentHost()
                + serverProperties.getContentReadEndPoint()
                + "/" + courseId
                + serverProperties.getContentReadEndPointFields();
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);
        if (MapUtils.isEmpty(response) || !Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            logger.warn("BPReportsServiceV2Impl:: fetchProgramDetails: Empty or error response for courseId: {}", courseId);
            return;
        }
        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
        if (MapUtils.isEmpty(result)) return;
        Map<String, Object> content = (Map<String, Object>) result.get(Constants.CONTENT);
        if (MapUtils.isNotEmpty(content)) {
            referenceData.put(Constants.PROGRAM_NAME, content.get(Constants.NAME));
            referenceData.put(Constants.PRIMARY_CATEGORY, content.get(Constants.PRIMARY_CATEGORY));
        }
    }

    /**
     * Fetches MDO / Department Name from the Org Search API using the orgId from batch createdFor.
     */
    private void fetchMdoName(String orgId, Map<String, Object> referenceData) {
        String url = serverProperties.getSbUrl() + serverProperties.getSbOrgSearchPath();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.ID, Collections.singletonList(orgId));
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.FILTERS, filters);
        requestBody.put(Constants.FIELDS_CONSTANT, Collections.singletonList(Constants.ORG_NAME));
        Map<String, Object> requestWrapper = Collections.singletonMap(Constants.REQUEST, requestBody);
        Map<String, String> headers = Collections.singletonMap(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
        Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPost(url, requestWrapper, headers);
        if (MapUtils.isEmpty(response) || !Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            logger.warn("BPReportsServiceV2Impl:: fetchMdoName: Empty or error response for orgId: {}", orgId);
            return;
        }
        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
        if (MapUtils.isEmpty(result)) return;
        Map<String, Object> apiResponse = (Map<String, Object>) result.get(Constants.RESPONSE);
        if (MapUtils.isEmpty(apiResponse)) return;
        List<Map<String, Object>> content = (List<Map<String, Object>>) apiResponse.get(Constants.CONTENT);
        if (!CollectionUtils.isEmpty(content)) {
            referenceData.put(Constants.MDO_NAME, content.get(0).get(Constants.ORG_NAME));
        }
    }

    /**
     * Fetches Program Coordinator Name from Cassandra user table using the batch's createdBy userId.
     * Only firstname is fetched — no full profile read needed for a display name.
     */
    private void fetchCoordinatorName(String userId, Map<String, Object> referenceData) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, userId);
        Map<String, Object> userDetails = cassandraOperation.getRecordsByProperties(
                Constants.SUNBIRD_KEY_SPACE_NAME,
                Constants.TABLE_USER,
                propertyMap,
                Collections.singletonList(Constants.FIRSTNAME),
                Constants.ID);
        if (MapUtils.isNotEmpty(userDetails)) {
            Map<String, Object> user = (Map<String, Object>) userDetails.get(userId);
            if (MapUtils.isNotEmpty(user)) {
                String name = (String) user.get(Constants.FIRSTNAME);
                referenceData.put(Constants.PROGRAM_COORDINATOR_NAME, name);
                referenceData.put(Constants.BATCH_CREATED_BY_NAME, name);
            }
        }
    }

    /**
     * Resolves instructor names from batch_attributes.sessionDetails_v2.
     * Each session may carry an instructorId; unique IDs are batch-fetched from Cassandra
     * and joined as a comma-separated string so multi-session batches show all instructors.
     * Safely returns nothing if the field is absent — the schema is still evolving.
     */
    private void fetchInstructorName(Map<String, Object> batchDetails, Map<String, Object> referenceData) {
        String batchAttributesStr = (String) batchDetails.get(Constants.BATCH_ATTRIBUTES);
        if (StringUtils.isBlank(batchAttributesStr)) {
            return;
        }
        Map<String, Object> batchAttributes;
        try {
            batchAttributes = objectMapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            logger.error("BPReportsServiceV2Impl:: fetchInstructorName: Failed to resolve instructor names", e);
            return;
        }
        List<Map<String, Object>> sessions =
                (List<Map<String, Object>>) batchAttributes.get(Constants.TABLE_COURSE_SESSION_DETAILS);
        if (CollectionUtils.isEmpty(sessions)) {
            return;
        }
        List<String> instructorIds = sessions.stream()
                .map(s -> (String) s.get(Constants.INSTRUCTOR_ID))
                .filter(StringUtils::isNotBlank)
                .distinct()
                .collect(Collectors.toList());
        if (instructorIds.isEmpty()) {
            logger.warn("BPReportsServiceV2Impl:: fetchInstructorName: No instructorId found in sessionDetails_v2");
            return;
        }
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ID, instructorIds);
        Map<String, Object> userDetails = cassandraOperation.getRecordsByProperties(
                Constants.SUNBIRD_KEY_SPACE_NAME,
                Constants.TABLE_USER,
                propertyMap,
                Collections.singletonList(Constants.FIRSTNAME),
                Constants.ID);
        if (MapUtils.isEmpty(userDetails)) {
            return;
        }
        List<String> names = instructorIds.stream()
                .map(id -> (Map<String, Object>) userDetails.get(id))
                .filter(MapUtils::isNotEmpty)
                .map(user -> (String) user.get(Constants.FIRSTNAME))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());
        if (!names.isEmpty()) {
            referenceData.put(Constants.INSTRUCTOR_NAME, String.join(", ", names));
        }
    }

    /**
     * Assembles a single user's report row from three pre-fetched data sources.
     * All lookups use in-memory maps rather than per-user DB calls — the batch
     * fetches upstream keep this method allocation-only (no I/O per user).
     */
    private Map<String, Object> buildUserData(WfStatusEntity entity,
                                              Map<String, Object> allUserProfiles,
                                              Map<String, List<Map<String, Object>>> allCertStatuses) {
        Map<String, Object> userData = new HashMap<>();
        String userId = entity.getUserId();
        userData.put(Constants.ENROLLMENT_STATUS, getEnrollmentStatus(entity.getCurrentStatus()));
        userData.put(Constants.CREATED_ON, entity.getCreatedOn());
        userData.put(Constants.DEPT_NAME, entity.getDeptName());
        parseEnrollmentFormData(entity.getUpdateFieldValues(), userData);
        applyUserProfile(userId, allUserProfiles, userData);
        applyCertificateStatus(userId, allCertStatuses, userData);
        return userData;
    }

    /**
     * Maps internal workflow status codes to the human-readable labels shown in the report.
     * Returns null for any value not in the known set, matching V1 behavior.
     */
    private String getEnrollmentStatus(String currentStatus) {
        if (Constants.SEND_FOR_MDO_APPROVAL.equalsIgnoreCase(currentStatus)) return Constants.PENDING_WITH_MDO;
        if (Constants.SEND_FOR_PC_APPROVAL.equalsIgnoreCase(currentStatus)) return Constants.PENDING_WITH_PC;
        if (Constants.APPROVED_UPPER_CASE.equalsIgnoreCase(currentStatus)) return Constants.APPROVED_UPPER_CASE;
        if (Constants.REJECTED_UPPER_CASE.equalsIgnoreCase(currentStatus)) return Constants.REJECTED_UPPER_CASE;
        return null;
    }

    /**
     * Parses updateFieldValues JSON from wf_status to extract designation and group
     * that the user filled during enrollment — distinct from their standing profile values.
     */
    private void parseEnrollmentFormData(String updateFieldValuesStr, Map<String, Object> userData) {
        if (StringUtils.isBlank(updateFieldValuesStr)) {
            return;
        }
        try {
            List<Map<String, Object>> fields = objectMapper.readValue(
                    updateFieldValuesStr, new TypeReference<List<Map<String, Object>>>() {
                    });
            for (Map<String, Object> field : fields) {
                String fieldKey = (String) field.get(Constants.FIELD_KEY);
                Object toValue = field.get(Constants.TO_VALUE);
                if (Constants.PROFILE_DETAILS_DESIGNATION.equals(fieldKey)) {
                    userData.put(Constants.DESIGNATION_TITLE_CASE, toValue);
                } else if (Constants.PROFILE_DETAILS_GROUP.equals(fieldKey)) {
                    userData.put(Constants.GROUP_TITLE_CASE, toValue);
                }
            }
        } catch (IOException e) {
            logger.error("BPReportsServiceV2Impl:: parseEnrollmentFormData: Failed to parse updateFieldValues", e);
        }
    }

    /**
     * Fetches user profiles in chunks to avoid unbounded Cassandra IN-clause scatter-gather.
     * Chunk size is controlled by bp.report.cassandra.in.chunk.size (default 100).
     * CassandraOperationImpl handles its own exceptions and returns empty on failure,
     * so no try-catch is needed here.
     */
    private Map<String, Object> batchFetchUserProfiles(List<String> userIds) {
        if (CollectionUtils.isEmpty(userIds)) return Collections.emptyMap();
        logger.debug("BPReportsServiceV2Impl:: batchFetchUserProfiles: Fetching profiles for {} users in chunks of {}",
                userIds.size(), serverProperties.getBpReportCassandraInChunkSize());
        Map<String, Object> combined = new HashMap<>();
        for (List<String> chunk : chunkList(userIds, serverProperties.getBpReportCassandraInChunkSize())) {
            Map<String, Object> result = cassandraOperation.getRecordsByProperties(
                    Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_USER,
                    Collections.singletonMap(Constants.ID, chunk), null, Constants.ID);
            if (MapUtils.isNotEmpty(result)) combined.putAll(result);
        }
        logger.debug("BPReportsServiceV2Impl:: batchFetchUserProfiles: Fetched profiles for {} users", combined.size());
        return combined;
    }

    /**
     * Fetches issued_certificates for all enrolled users in chunked IN-clause queries.
     * Groups results by userId so callers can do O(1) lookup per user.
     * Users with no certificate entry are absent from the returned map, not mapped to null.
     */
    private Map<String, List<Map<String, Object>>> batchFetchCertificateStatuses(
            List<String> userIds, String courseId, String batchId) {
        if (CollectionUtils.isEmpty(userIds)) return Collections.emptyMap();
        Map<String, List<Map<String, Object>>> combined = new HashMap<>();
        for (List<String> chunk : chunkList(userIds, serverProperties.getBpReportCassandraInChunkSize())) {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID, chunk);
            propertyMap.put(Constants.COURSE_ID, courseId);
            propertyMap.put(Constants.BATCH_ID, batchId);
            List<Map<String, Object>> rows = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.TABLE_USER_ENROLMENT,
                    propertyMap, Arrays.asList(Constants.USER_ID, Constants.ISSUED_CERTIFICATES));
            if (!CollectionUtils.isEmpty(rows)) {
                for (Map<String, Object> row : rows) {
                    String uid = (String) row.get(Constants.USER_ID);
                    Object certs = row.get(Constants.ISSUED_CERTIFICATES);
                    combined.put(uid, certs instanceof List
                            ? (List<Map<String, Object>>) certs : Collections.emptyList());
                }
            }
        }
        return combined;
    }

    /**
     * Splits a list into fixed-size sub-lists using subList views (no copying).
     * Used to keep Cassandra IN-clause partition-key counts within safe scatter-gather bounds.
     */
    private <T> List<List<T>> chunkList(List<T> list, int size) {
        List<List<T>> chunks = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            chunks.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return chunks;
    }

    /**
     * Merges a user's Cassandra profile into their report row.
     * Parses the profileDetails JSON blob and delegates each section to a focused helper.
     * Parse failure is logged and the row retains only the firstname fetched as a top-level column.
     */
    private void applyUserProfile(String userId, Map<String, Object> allUserProfiles, Map<String, Object> userData) {
        Map<String, Object> user = (Map<String, Object>) allUserProfiles.get(userId);
        if (MapUtils.isEmpty(user)) {
            logger.warn("BPReportsServiceV2Impl:: applyUserProfile: No profile found for userId: {}", userId);
            return;
        }
        userData.put(Constants.FIRSTNAME, user.get(Constants.FIRSTNAME));
        String profileDetailsStr = (String) user.get(Constants.PROFILE_DETAILS_LOWER);
        if (StringUtils.isBlank(profileDetailsStr)) {
            logger.warn("BPReportsServiceV2Impl:: applyUserProfile: profileDetails is blank for userId: {}", userId);
            return;
        }
        Map<String, Object> profileDetails;
        try {
            profileDetails = objectMapper.readValue(profileDetailsStr, new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            logger.error("BPReportsServiceV2Impl:: applyUserProfile: Failed to parse profileDetails for userId: {}", userId, e);
            return;
        }
        applyPersonalDetails(profileDetails, userData);
        applyProfessionalDetails(profileDetails, userData);
        applyEmploymentDetails(profileDetails, userData);
        applyAdditionalProperties(profileDetails, userData);
        applyCadreDetails(profileDetails, userData);
    }

    /**
     * Copies personal fields (email, mobile, gender, DOB, domicile, category) from profileDetails into the user row.
     */
    private void applyPersonalDetails(Map<String, Object> profileDetails, Map<String, Object> userData) {
        Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
        if (MapUtils.isEmpty(personalDetails)) return;
        userData.put(Constants.PRIMARY_EMAIL, personalDetails.get(Constants.PRIMARY_EMAIL));
        userData.put(Constants.MOBILE, personalDetails.get(Constants.MOBILE));
        userData.put(Constants.GENDER, personalDetails.get(Constants.GENDER));
        userData.put(Constants.DOB, personalDetails.get(Constants.DOB));
        userData.put(Constants.DOMICILE_MEDIUM, personalDetails.get(Constants.DOMICILE_MEDIUM));
        userData.put(Constants.CATEGORY, personalDetails.get(Constants.CATEGORY));
    }

    /**
     * Copies group, designation (each appended with verification status label), and DOR
     * from the first professionalDetails entry into the user row.
     */
    private void applyProfessionalDetails(Map<String, Object> profileDetails, Map<String, Object> userData) {
        String groupStatus = resolveVerificationStatus((String) profileDetails.get(Constants.PROFILE_GROUP_STATUS));
        String designationStatus = resolveVerificationStatus((String) profileDetails.get(Constants.PROFILE_DESIGNATION_STATUS));
        List<Map<String, Object>> professionalDetails =
                (List<Map<String, Object>>) profileDetails.get(Constants.PROFESSIONAL_DETAILS);
        if (CollectionUtils.isEmpty(professionalDetails)) return;
        Map<String, Object> prof = professionalDetails.get(0);
        String group = StringUtils.isNotEmpty((String) prof.get(Constants.GROUP))
                ? prof.get(Constants.GROUP) + " (" + groupStatus + ")" : "";
        String designation = StringUtils.isNotEmpty((String) prof.get(Constants.DESIGNATION))
                ? prof.get(Constants.DESIGNATION) + " (" + designationStatus + ")" : "";
        userData.put(Constants.GROUP, group);
        userData.put(Constants.DESIGNATION, designation);
        userData.put(Constants.DOR, prof.get(Constants.DOR));
    }

    /**
     * Returns Verified/Not-Verified label for a raw status string, replicating V1 logic.
     */
    private String resolveVerificationStatus(String status) {
        return StringUtils.isNotEmpty(status) && Constants.VERIFIED.equalsIgnoreCase(status)
                ? Constants.VERIFIED_TITLE_CASE : Constants.NOT_VERIFIED_TITLE_CASE;
    }

    /**
     * Copies department name, employee code, and pincode from employmentDetails into the user row.
     */
    private void applyEmploymentDetails(Map<String, Object> profileDetails, Map<String, Object> userData) {
        Map<String, Object> employmentDetails = (Map<String, Object>) profileDetails.get(Constants.EMPLOYMENT_DETAILS);
        if (MapUtils.isEmpty(employmentDetails)) return;
        userData.put(Constants.DEPARTMENTNAME, employmentDetails.get(Constants.DEPARTMENTNAME));
        userData.put(Constants.EMPLOYEE_CODE, employmentDetails.get(Constants.EMPLOYEE_CODE));
        userData.put(Constants.PINCODE, employmentDetails.get(Constants.PINCODE));
    }

    /**
     * Copies externalSystemId from additionalProperties into the user row.
     */
    private void applyAdditionalProperties(Map<String, Object> profileDetails, Map<String, Object> userData) {
        Map<String, Object> additionalProperties = (Map<String, Object>) profileDetails.get(Constants.ADDITIONAL_PROPERTIES);
        if (MapUtils.isEmpty(additionalProperties)) return;
        userData.put(Constants.EXTERNAL_SYSTEM_ID, additionalProperties.get(Constants.EXTERNAL_SYSTEM_ID));
    }

    /**
     * Sets cadreDetails flag and related fields. Writes "No" when the key exists but value is null/empty
     * (user explicitly cleared cadre); writes "Yes" with all sub-fields when cadre data is present.
     */
    private void applyCadreDetails(Map<String, Object> profileDetails, Map<String, Object> userData) {
        Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(Constants.CADRE_DETAILS);
        if (profileDetails.containsKey(Constants.CADRE_DETAILS) && MapUtils.isEmpty(cadreDetails)) {
            userData.put(Constants.CADRE_DETAILS, Constants.NO);
        } else if (MapUtils.isNotEmpty(cadreDetails)) {
            userData.put(Constants.CADRE_DETAILS, Constants.YES);
            userData.put(Constants.CIVIL_SERVICE_TYPE, cadreDetails.get(Constants.CIVIL_SERVICE_TYPE));
            userData.put(Constants.CIVIL_SERVICE_NAME, cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
            userData.put(Constants.CADRE_NAME, cadreDetails.get(Constants.CADRE_NAME));
            userData.put(Constants.CADRE_BATCH, cadreDetails.get(Constants.CADRE_BATCH));
            userData.put(Constants.CONTROLLING_AUTHORITY, cadreDetails.get(Constants.CONTROLLING_AUTHORITY));
        }
    }

    /**
     * Sets CERTIFICATE_ISSUED (Yes/No) and, when issued, the issue date on the user row.
     * Reads from the pre-fetched allCertStatuses map — no extra DB call per user.
     */
    private void applyCertificateStatus(String userId,
                                        Map<String, List<Map<String, Object>>> allCertStatuses,
                                        Map<String, Object> userData) {
        List<Map<String, Object>> certs = allCertStatuses.get(userId);
        if (CollectionUtils.isEmpty(certs)) {
            userData.put(Constants.CERTIFICATE_ISSUED, Constants.NO);
        } else {
            userData.put(Constants.CERTIFICATE_ISSUED, Constants.YES);
            userData.put(Constants.CERTIFICATE_ISSUED_DATE, certs.get(0).get(Constants.LAST_ISSUED_ON));
        }
    }

    /**
     * Writes the finished workbook to a temp file, uploads it to cloud storage, then
     * records the download URL and enrolment counts in Cassandra.
     * Any I/O or upload failure marks the report FAILED so the requester can re-trigger.
     */
    private void uploadReportAndUpdateDatabase(Workbook workbook, String orgId, String courseId,
                                               String batchId, String reportRequester,
                                               int[] counts, String contextType) {
        String fileName = serverProperties.getBpReportV2FileNamePrefix() + System.currentTimeMillis() + "_" + batchId + ".xlsx";
        String localDir = Constants.LOCAL_BASE_PATH + Constants.BP_REPORT_LOCAL_SUBDIR + orgId + "/" + courseId + "/";
        File directory = new File(localDir);
        if (!directory.exists() && !directory.mkdirs()) {
            logger.error("BPReportsServiceV2Impl:: Failed to create local directory: {}", localDir);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, contextType);
            return;
        }
        File file = new File(directory, fileName);
        try (OutputStream fileOut = Files.newOutputStream(file.toPath())) {
            workbook.write(fileOut);
        } catch (IOException e) {
            logger.error("BPReportsServiceV2Impl:: Failed to write Excel file: {}", fileName, e);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, contextType);
            deleteLocalFile(file);
            return;
        }
        logger.info("BPReportsServiceV2Impl:: uploadReportAndUpdateDatabase: Excel written locally: {}", file.getAbsolutePath());
        String cloudPath = String.join("/", serverProperties.getBpEnrolmentReportContainerName(), orgId, courseId, batchId);
        SBApiResponse uploadResponse = storageService.uploadFile(file, cloudPath, serverProperties.getCloudContainerName());
        String downloadUrl = (String) uploadResponse.getResult().get(Constants.URL);
        if (StringUtils.isBlank(downloadUrl)) {
            logger.error("BPReportsServiceV2Impl:: Cloud upload returned no URL for batchId: {}", batchId);
            updateReportStatus(orgId, courseId, batchId, reportRequester, Constants.FAILED_UPPERCASE, contextType);
            deleteLocalFile(file);
            return;
        }
        logger.info("BPReportsServiceV2Impl:: uploadReportAndUpdateDatabase: Report uploaded to cloud for batchId: {}, url: {}", batchId, downloadUrl);
        deleteLocalFile(file);
        Map<String, Object> compositeKey = buildCompositeKey(orgId, courseId, batchId, reportRequester, contextType);
        updateFinalReportStatus(compositeKey, downloadUrl, fileName, counts);
    }

    /**
     * Deletes the local temp Excel file after cloud upload — prevents disk accumulation across report runs.
     */
    private void deleteLocalFile(File file) {
        try {
            Files.delete(file.toPath());
        } catch (IOException e) {
            logger.warn("BPReportsServiceV2Impl:: deleteLocalFile: Failed to delete local file: {}", file.getAbsolutePath(), e);
        }
    }

    /**
     * Tallies enrolled users into pending / approved / rejected buckets in one pass.
     * Everything that is not explicitly APPROVED or REJECTED is counted as pending,
     * which covers all intermediate MDO/PC workflow states.
     */
    private int[] computeEnrolmentCounts(List<WfStatusEntity> enrolledUsers) {
        int pending = 0;
        int approved = 0;
        int rejected = 0;
        for (WfStatusEntity entity : enrolledUsers) {
            if (Constants.APPROVED_UPPER_CASE.equalsIgnoreCase(entity.getCurrentStatus())) approved++;
            else if (Constants.REJECTED_UPPER_CASE.equalsIgnoreCase(entity.getCurrentStatus())) rejected++;
            else pending++;
        }
        return new int[]{pending, approved, rejected};
    }

    /**
     * Marks the report row COMPLETED in Cassandra and records the download URL,
     * filename, generation timestamp, and per-status enrolment counts.
     * Status is always COMPLETED here — FAILED paths call updateReportStatus directly.
     */
    private void updateFinalReportStatus(Map<String, Object> compositeKey, String downloadUrl,
                                         String fileName, int[] counts) {
        Map<String, Object> updateAttributes = new HashMap<>();
        updateAttributes.put(Constants.STATUS, Constants.COMPLETED_UPPER_CASE);
        updateAttributes.put(Constants.PENDING_USER, counts[0]);
        updateAttributes.put(Constants.APPROVED_USER, counts[1]);
        updateAttributes.put(Constants.REJECTED_USER, counts[2]);
        updateAttributes.put(Constants.LAST_REPORT_GENERATED_ON, new Date());
        updateAttributes.put(Constants.DOWNLOAD_LINK, downloadUrl);
        updateAttributes.put(Constants.FILE_NAME, fileName);
        cassandraOperation.updateRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE_V2,
                updateAttributes, compositeKey);
    }

    /**
     * Creates the "Enrollment Report" sheet, writes a bold header row, then writes
     * one data row per assembled user map.  Row numbering starts at 1 (0 is the header).
     */
    private void buildExcelSheet(Workbook workbook, Map<String, Object> batchDetails,
                                 Map<String, Object> referenceData, int totalEnrolled,
                                 List<Map<String, Object>> userDataList) {
        Sheet sheet = workbook.createSheet(serverProperties.getBpReportSheetName());
        createHeaderRow(workbook, sheet);
        int rowNum = 1;
        for (Map<String, Object> userData : userDataList) {
            writeDataRow(sheet.createRow(rowNum++), batchDetails, referenceData, totalEnrolled, userData);
        }
    }

    /**
     * Writes the header row (row 0) using the column list from server properties,
     * styled bold and left-aligned with text wrap enabled.
     */
    private void createHeaderRow(Workbook workbook, Sheet sheet) {
        CellStyle headerStyle = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        headerStyle.setFont(font);
        headerStyle.setAlignment(HorizontalAlignment.LEFT);
        headerStyle.setWrapText(true);
        Row headerRow = sheet.createRow(0);
        List<String> headers = serverProperties.getBpReportV2Headers();
        for (int i = 0; i < headers.size(); i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers.get(i));
            cell.setCellStyle(headerStyle);
        }
    }

    /**
     * Fills a single Excel row: first 16 columns are batch/program-level (same for every row),
     * remaining 30 columns are per-user.  Column order must match the header list from
     * {@code serverProperties.getBpReportV2Headers()}.
     */
    private void writeDataRow(Row row, Map<String, Object> batchDetails, Map<String, Object> referenceData,
                              int totalEnrolled, Map<String, Object> userData) {
        int col = 0;
        // Batch-level columns (16)
        setCellValue(row, col++, batchDetails.get(Constants.BATCH_ID));
        setCellValue(row, col++, batchDetails.get(Constants.NAME));
        setCellValue(row, col++, batchDetails.get(Constants.COURSE_ID));
        setCellValue(row, col++, referenceData.get(Constants.PROGRAM_NAME));
        setCellValue(row, col++, referenceData.get(Constants.PRIMARY_CATEGORY));
        setCellValue(row, col++, referenceData.get(Constants.MDO_NAME));
        setCellValue(row, col++, referenceData.get(Constants.PROGRAM_COORDINATOR_NAME));
        setCellValue(row, col++, batchDetails.get(Constants.STATUS));
        setCellValue(row, col++, batchDetails.get(Constants.START_DATE_COLUMN));
        setCellValue(row, col++, batchDetails.get(Constants.END_DATE_COLUMN));
        setCellValue(row, col++, batchDetails.get(Constants.START_DATE_COLUMN));
        setCellValue(row, col++, batchDetails.get(Constants.ENROLMENT_END_DATE_COLUMN));
        setCellValue(row, col++, computeBatchDuration(batchDetails));
        setCellValue(row, col++, totalEnrolled);
        setCellValue(row, col++, batchDetails.get(Constants.CREATED_DATE));
        setCellValue(row, col++, referenceData.get(Constants.BATCH_CREATED_BY_NAME));
        // Per-user columns (30)
        setCellValue(row, col++, userData.get(Constants.FIRSTNAME));
        setCellValue(row, col++, userData.get(Constants.DEPARTMENTNAME));
        setCellValue(row, col++, userData.get(Constants.GROUP));
        setCellValue(row, col++, userData.get(Constants.DESIGNATION));
        setCellValue(row, col++, userData.get(Constants.EMPLOYEE_CODE));
        setCellValue(row, col++, userData.get(Constants.PRIMARY_EMAIL));
        setCellValue(row, col++, userData.get(Constants.MOBILE));
        setCellValue(row, col++, userData.get(Constants.GENDER));
        setCellValue(row, col++, userData.get(Constants.DOB));
        setCellValue(row, col++, userData.get(Constants.DOMICILE_MEDIUM));
        setCellValue(row, col++, userData.get(Constants.CATEGORY));
        setCellValue(row, col++, userData.get(Constants.PINCODE));
        setCellValue(row, col++, userData.get(Constants.CADRE_DETAILS));
        setCellValue(row, col++, userData.get(Constants.CIVIL_SERVICE_TYPE));
        setCellValue(row, col++, userData.get(Constants.CIVIL_SERVICE_NAME));
        setCellValue(row, col++, userData.get(Constants.CADRE_NAME));
        setCellValue(row, col++, userData.get(Constants.CADRE_BATCH));
        setCellValue(row, col++, userData.get(Constants.CONTROLLING_AUTHORITY));
        setCellValue(row, col++, userData.get(Constants.EXTERNAL_SYSTEM_ID));
        setCellValue(row, col++, userData.get(Constants.DOR));
        setCellValue(row, col++, userData.get(Constants.ENROLLMENT_STATUS));
        setCellValue(row, col++, userData.get(Constants.DESIGNATION_TITLE_CASE));
        setCellValue(row, col++, userData.get(Constants.GROUP_TITLE_CASE));
        setCellValue(row, col++, userData.get(Constants.FIRSTNAME));           // Learner Name = Name
        setCellValue(row, col++, userData.get(Constants.PRIMARY_EMAIL));       // Email ID = Email
        setCellValue(row, col++, userData.get(Constants.DEPT_NAME));
        setCellValue(row, col++, userData.get(Constants.CREATED_ON));
        setCellValue(row, col++, referenceData.get(Constants.INSTRUCTOR_NAME));
        setCellValue(row, col++, userData.get(Constants.CERTIFICATE_ISSUED));
        setCellValue(row, col, userData.get(Constants.CERTIFICATE_ISSUED_DATE));
    }

    /**
     * Writes an Object cell value, routing through {@link #formatCellValue} for type-safe string conversion.
     */
    private void setCellValue(Row row, int col, Object value) {
        row.createCell(col).setCellValue(formatCellValue(value));
    }

    /**
     * Writes a numeric cell value as a native POI numeric cell (no string conversion).
     */
    private void setCellValue(Row row, int col, int value) {
        row.createCell(col).setCellValue(value);
    }

    /**
     * Converts an arbitrary report field to a trimmed string for Excel.
     * {@code Date} values are formatted as ISO local-date (yyyy-MM-dd); nulls become "".
     */
    private String formatCellValue(Object value) {
        if (Objects.isNull(value)) return "";
        if (value instanceof Date) {
            return ((Date) value).toInstant()
                    .atZone(java.time.ZoneId.systemDefault())
                    .toLocalDate()
                    .toString();
        }
        return String.valueOf(value).trim();
    }

    /**
     * Returns the number of calendar days between batch start and end as a string,
     * or an empty string if either date is missing or unparseable.
     */
    private String computeBatchDuration(Map<String, Object> batchDetails) {
        LocalDate start = parseToLocalDate(batchDetails.get(Constants.START_DATE_COLUMN));
        LocalDate end = parseToLocalDate(batchDetails.get(Constants.END_DATE_COLUMN));
        if (Objects.nonNull(start) && Objects.nonNull(end)) {
            return String.valueOf(ChronoUnit.DAYS.between(start, end));
        }
        return "";
    }

    /**
     * Normalises a date field (LocalDate, java.util.Date, or ISO-8601 String) to LocalDate.
     * Strings longer than 10 characters are truncated to their date prefix before parsing,
     * handling the common "2024-01-15T00:00:00" format stored in Cassandra.
     * Returns null for null, blank, or unparseable input rather than throwing.
     */
    private LocalDate parseToLocalDate(Object dateObj) {
        if (Objects.isNull(dateObj)) return null;
        if (dateObj instanceof LocalDate) return (LocalDate) dateObj;
        if (dateObj instanceof Date) {
            return ((Date) dateObj).toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate();
        }
        if (dateObj instanceof String) {
            String s = ((String) dateObj).trim();
            if (s.isEmpty()) return null;
            try {
                return LocalDate.parse(s.length() > 10 ? s.substring(0, 10) : s);
            } catch (java.time.format.DateTimeParseException e) {
                logger.warn("BPReportsServiceV2Impl:: parseToLocalDate: Unparseable date string: {}", s);
                return null;
            }
        }
        return null;
    }

    /**
     * Fetches batch metadata from course_batch for the V2 report header section.
     * Requests all columns needed for the report — name, dates, status, creator —
     * rather than the minimal set used by V1 (which only needs batch_attributes and createdFor).
     * Returns an empty map (not null) if the batch is not found, so callers can use isEmpty().
     */
    private Map<String, Object> fetchBatchDetails(String courseId, String batchId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.COURSE_ID, courseId);
        propertyMap.put(Constants.BATCH_ID, batchId);
        List<Map<String, Object>> results = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.SUNBIRD_COURSES_KEY_SPACE_NAME,
                Constants.TABLE_COURSE_BATCH,
                propertyMap,
                serverProperties.getBpReportBatchDetailFields());
        return CollectionUtils.isEmpty(results) ? Collections.emptyMap() : results.get(0);
    }

    /**
     * Returns the current status of a V2 BP report for a given batch and requester.
     * Queries bp_enrolment_report_v2 using orgId, courseId, batchId, reportRequester,
     * and contextType as composite key — ensuring V2 records are never mixed with V1.
     * Returns the report record (status, downloadLink, enrolment counts) if found,
     * or an informational error asking the caller to trigger generation first.
     */
    @Override
    public SBApiResponse getBPReportStatusV2(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_BP_REPORT_STATUS_V2);
        try {
            String userId = validateUserToken(authToken, response);
            if (StringUtils.isBlank(userId)) {
                logger.warn("BPReportsServiceV2Impl:: getBPReportStatusV2: Invalid or missing auth token");
                return response;
            }
            logger.info("BPReportsServiceV2Impl:: getBPReportStatusV2: Started for userId: {}", userId);
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            String orgId = (String) request.get(Constants.ORG_ID);
            String courseId = (String) request.get(Constants.COURSE_ID);
            String batchId = (String) request.get(Constants.BATCH_ID);
            String reportRequester = (String) request.get(Constants.REPORT_REQUESTER);
            logger.debug("BPReportsServiceV2Impl:: getBPReportStatusV2: Fetching status for courseId: {}, batchId: {}, reportRequester: {}", courseId, batchId, reportRequester);
            if (!validateUserOrganization(userId, orgId, response)) {
                logger.warn("BPReportsServiceV2Impl:: getBPReportStatusV2: User {} does not belong to orgId: {}", userId, orgId);
                return response;
            }
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ORG_ID, orgId);
            propertyMap.put(Constants.COURSE_ID, courseId);
            propertyMap.put(Constants.BATCH_ID, batchId);
            propertyMap.put(Constants.REPORT_REQUESTER, reportRequester);
            propertyMap.put(Constants.CONTEXT_TYPE, Constants.BP_REPORT_V2_CONTEXT_TYPE);
            List<Map<String, Object>> reportList = cassandraOperation.getRecordsByProperties(
                    Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.BP_ENROLMENT_REPORT_TABLE_V2,
                    propertyMap, null);
            if (CollectionUtils.isEmpty(reportList)) {
                logger.info("BPReportsServiceV2Impl:: getBPReportStatusV2: No report found for batchId: {}, reportRequester: {}", batchId, reportRequester);
                updateErrorDetails(response, "Report is not available. Please generate the report.", HttpStatus.OK);
                return response;
            }
            logger.info("BPReportsServiceV2Impl:: getBPReportStatusV2: Report status fetched successfully for batchId: {}, count: {}", batchId, reportList.size());
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.CONTENT, reportList);
            response.getResult().put(Constants.COUNT, reportList.size());
        } catch (Exception e) {
            logger.error("BPReportsServiceV2Impl:: getBPReportStatusV2: Failed to fetch report status", e);
            updateErrorDetails(response, "Failed to get BP report status. Error: " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }
}
