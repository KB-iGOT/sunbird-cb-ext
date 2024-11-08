package org.sunbird.bpreports.service;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.cassandra.utils.CassandraOperationImpl;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.storage.service.StorageService;
import org.sunbird.user.service.UserUtilityService;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class BPReportsServiceImpl implements BPReportsService {

    private static final Logger logger = LoggerFactory.getLogger(BPReportsServiceImpl.class);

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    private UserUtilityService userUtilityService;

    @Autowired
    private CassandraOperationImpl cassandraOperation;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    StorageService storageService;

    @Override
    public SBApiResponse generateBPReport(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, "Invalid user ID from auth token.", HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            SBApiResponse errResponse = validateGenerateReportRequestBody(request);
            if (!ObjectUtils.isEmpty(errResponse)) {
                return errResponse;
            }

            String courseId = (String) request.get(Constants.COURSE_ID);
            String batchId = (String) request.get(Constants.BATCH_ID);
            String orgId = (String) request.get(Constants.ORG_ID);

            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Arrays.asList(userId),
                    Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID), userInfoMap);
            String userOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
            if (StringUtils.isBlank(userOrgId) || !userOrgId.equals(orgId)) {
                updateErrorDetails(response, "Requested User is not belongs to same organisation.", HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put(Constants.ORG_ID, orgId);
            keyMap.put(Constants.COURSE_ID, courseId);
            keyMap.put(Constants.BATCH_ID, batchId);

            List<Map<String, Object>> existingReportDetails = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    Constants.BP_ENROLMENT_REPORT_TABLE, keyMap, null);

            if (!CollectionUtils.isEmpty(existingReportDetails)) {
                String status = (String) existingReportDetails.get(0).get(Constants.STATUS);
                if (Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase(status)) {
                    response.getParams().setStatus(Constants.SUCCESS);
                    response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                    response.setResponseCode(HttpStatus.OK);
                    return response;
                } else {
                    logger.info("Update BP report details::started");
                    return updateReportDetailsInDBAndTriggerKafkaEvent(userId, request);

                }

            } else {
                logger.info("Insert BP report details into DB::started");
                return insertReportDetailsInDBAndTriggerKafkaEvent(userId, request);
            }


        } catch (Exception e) {
            logger.error("Error while processing the request", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    private SBApiResponse validateGenerateReportRequestBody(Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        if (CollectionUtils.isEmpty(requestBody)) {
            updateErrorDetails(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.COURSE_ID))) {
            updateErrorDetails(response, Constants.COURSE_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.BATCH_ID))) {
            updateErrorDetails(response, Constants.BATCH_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.ORG_ID))) {
            updateErrorDetails(response, Constants.ORG_ID_KEY_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.SURVEY_ID))) {
            updateErrorDetails(response, Constants.SURVEY_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        return null;
    }

    private void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

    private SBApiResponse insertReportDetailsInDBAndTriggerKafkaEvent(String userId, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);

        try {
            Map<String, Object> dbRequest = new HashMap<>();
            dbRequest.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
            dbRequest.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
            dbRequest.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));
            dbRequest.put(Constants.SURVEY_ID, requestBody.get(Constants.SURVEY_ID));
            dbRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            dbRequest.put(Constants.CREATED_BY, userId);
            SBApiResponse dbResponse = cassandraOperation.insertRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, dbRequest);

            if (dbResponse.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                Map<String, Object> kafkaRequest = new HashMap<>();
                kafkaRequest.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
                kafkaRequest.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
                kafkaRequest.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));
                kafkaRequest.put(Constants.SURVEY_ID, requestBody.get(Constants.SURVEY_ID));
                kafkaRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                kafkaRequest.put(Constants.CREATED_BY, userId);
                kafkaProducer.push(serverProperties.getKafkaTopicBPReport(), kafkaRequest);

                response.getResult().put(Constants.STATUS, Constants.SUCCESS);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            } else {
                logger.error("Error while inserting record in the DB");
                updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while inserting record in the DB", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;

        }
        logger.info("Insert BP report details into DB::started");
        return response;

    }

    private SBApiResponse updateReportDetailsInDBAndTriggerKafkaEvent(String userId, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        try {
            Map<String, Object> compositeKey = new HashMap<>();
            Map<String, Object> updateAttributes = new HashMap<>();
            compositeKey.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
            compositeKey.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
            compositeKey.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));

            updateAttributes.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            updateAttributes.put(Constants.CREATED_BY, userId);
            updateAttributes.put(Constants.FILE_NAME, null);
            updateAttributes.put(Constants.DOWNLOAD_LINK, null);
            updateAttributes.put(Constants.PENDING_USER, 0);
            updateAttributes.put(Constants.REJECTED_USER, 0);
            updateAttributes.put(Constants.APPROVED_USER, 0);
            Map<String, Object> updateResponse = cassandraOperation.updateRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, updateAttributes, compositeKey);

            if (updateResponse.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                Map<String, Object> kafkaRequest = new HashMap<>();
                kafkaRequest.put(Constants.ORG_ID, requestBody.get(Constants.ORG_ID));
                kafkaRequest.put(Constants.COURSE_ID, requestBody.get(Constants.COURSE_ID));
                kafkaRequest.put(Constants.BATCH_ID, requestBody.get(Constants.BATCH_ID));
                kafkaRequest.put(Constants.SURVEY_ID, requestBody.get(Constants.SURVEY_ID));
                kafkaRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                kafkaRequest.put(Constants.CREATED_BY, userId);
                kafkaProducer.push(serverProperties.getKafkaTopicBPReport(), kafkaRequest);

                response.getResult().put(Constants.STATUS, Constants.SUCCESS);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            } else {
                logger.error("Error while inserting record in the DB");
                updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

        } catch (Exception e) {
            logger.error("Error while inserting record in the DB", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;

        }
        logger.info("Updating BP report details::end");
        return response;
    }

    @Override
    public SBApiResponse getBPReportStatus(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_ENROLLMENT_BP_REPORT_STATUS);

        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            if (StringUtils.isBlank(userId)) {
                updateErrorDetails(response, "Invalid user ID from auth token.", HttpStatus.BAD_REQUEST);
                return response;
            }
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            SBApiResponse errResponse = validateReportStatusRequestBody(request);
            if (!ObjectUtils.isEmpty(errResponse)) {
                return errResponse;
            }

            String courseId = (String) request.get(Constants.COURSE_ID);
            String batchId = (String) request.get(Constants.BATCH_ID);
            String orgId = (String) request.get(Constants.ORG_ID);

            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Arrays.asList(userId),
                    Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID), userInfoMap);
            String userOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
            if (StringUtils.isBlank(userOrgId) || !userOrgId.equals(orgId)) {
                updateErrorDetails(response, "Requested User is not belongs to same organisation.", HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ORG_ID, orgId);
            propertyMap.put(Constants.COURSE_ID, courseId);
            propertyMap.put(Constants.BATCH_ID, batchId);
            List<Map<String, Object>> reportList = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.BP_ENROLMENT_REPORT_TABLE, propertyMap, null);
            if (CollectionUtils.isEmpty(reportList)) {
                updateErrorDetails(response, "Report is not available. Please generate the report", HttpStatus.OK);
                return response;
            } else {
                response.getParams().setStatus(Constants.SUCCESSFUL);
                response.setResponseCode(HttpStatus.OK);
                response.getResult().put(Constants.CONTENT, reportList);
                response.getResult().put(Constants.COUNT, reportList.size());
            }

        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to get bp report status. Error: ", e.getMessage()));
        }
        return response;
    }

    public ResponseEntity<Resource> downloadBPReport(String authToken, String orgId, String courseId, String batchId, String fileName) {
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            // Check if userId is valid
            if (StringUtils.isBlank(userId)) {
                return createErrorResponse("Invalid user ID from auth token.", HttpStatus.UNAUTHORIZED);
            }

            try {
                // Download the file from storage
                String cloudBaseFolder = serverProperties.getBpEnrolmentReportContainerName();
                String cloudFilePath = cloudBaseFolder + "/" + orgId + "/" + courseId + "/" + batchId;
                storageService.downloadFile(fileName, cloudFilePath);
                Path tmpPath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);

                // Convert file to ByteArrayResource
                ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(tmpPath));

                // Determine content type based on file extension
                String contentType;
                if (fileName.endsWith(".xlsx")) {
                    contentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
                } else if (fileName.endsWith(".csv")) {
                    contentType = "text/csv";
                } else {
                    contentType = MediaType.APPLICATION_OCTET_STREAM_VALUE; // Default for unknown types
                }

                // Prepare headers for file download
                HttpHeaders headers = new HttpHeaders();
                headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");

                return ResponseEntity.ok()
                        .headers(headers)
                        .contentLength(Files.size(tmpPath))
                        .contentType(MediaType.parseMediaType(contentType))
                        .body(resource);
            } catch (IOException e) {
                logger.error("Failed to download the file: {}, Exception: {}", fileName, e);
                return createErrorResponse("Failed to download the requested file. Please try again later.", HttpStatus.INTERNAL_SERVER_ERROR);
            } finally {
                // Ensure file cleanup after download
                cleanupFile(Constants.LOCAL_BASE_PATH + fileName);
            }
        } catch (Exception e) {
            logger.error("Unexpected error while processing the download request for file: {}, Exception: {}", fileName, e);
            return createErrorResponse("An unexpected error occurred while processing your request. Please contact support.", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private SBApiResponse validateReportStatusRequestBody(Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        if (CollectionUtils.isEmpty(requestBody)) {
            updateErrorDetails(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.COURSE_ID))) {
            updateErrorDetails(response, Constants.COURSE_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.BATCH_ID))) {
            updateErrorDetails(response, Constants.BATCH_ID_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        if (StringUtils.isEmpty((String) requestBody.get(Constants.ORG_ID))) {
            updateErrorDetails(response, Constants.ORG_ID_KEY_MISSING, HttpStatus.BAD_REQUEST);
            return response;
        }
        return null;
    }

    private void setErrorData(SBApiResponse response, String errMsg) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ResponseEntity<Resource> createErrorResponse(String message, HttpStatus status) {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

        return ResponseEntity.status(status)
                .headers(headers)
                .body(new ByteArrayResource(message.getBytes()));
    }

    private void cleanupFile(String filePath) {
        try {
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
            }
        } catch (Exception e) {
            logger.warn("Failed to delete temporary file: {}, Exception: {}", filePath, e);
        }
    }

}
