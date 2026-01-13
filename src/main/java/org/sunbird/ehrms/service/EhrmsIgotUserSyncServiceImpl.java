package org.sunbird.ehrms.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;

import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.*;

@Service
public class EhrmsIgotUserSyncServiceImpl implements EhrmsIgotUserSyncService {

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    Producer kafkaProducer;

    @Value("${ehrms.user.data.sync.kafka.topic}")
    private String ehrmsUserDataSyncKafkaTopic;

    @Override
    public SBApiResponse userEhrmsDataUpdate(Map<String, Object> requestBody) throws Exception {
        SBApiResponse response = ProjectUtil.createDefaultResponse("ajdgja");
        try {
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            if (!request.containsKey("fromDate") || !request.containsKey("toDate")) {
                throw new IllegalArgumentException("fromDate and toDate are required");
            }
            if (!(request.get("fromDate") instanceof String) || !(request.get("toDate") instanceof String)) {
                throw new IllegalArgumentException("fromDate and toDate must be String in format dd-MM-yy");
            }
            String fromDateStr = request.get("fromDate").toString().trim();
            String toDateStr = request.get("toDate").toString().trim();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yy").withResolverStyle(ResolverStyle.STRICT);
            LocalDate from;
            LocalDate to;

            try {
                from = LocalDate.parse(fromDateStr, formatter);
                to = LocalDate.parse(toDateStr, formatter);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("Invalid date format. Expected dd-MM-yy", e);
            }
            if (to.isBefore(from)) {
                throw new IllegalArgumentException("toDate cannot be before fromDate");
            }
            LocalDate maxAllowedTo = from.plusMonths(6);

            if (to.isAfter(maxAllowedTo)) {
                throw new IllegalArgumentException("Date range cannot exceed 6 months");
            }
            String finalFromDate = from.format(formatter);
            String finalToDate = to.format(formatter);

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put("job_name", "EHRMS_SYNC");
            keyMap.put("status", Constants.STATUS_IN_PROGRESS_UPPERCASE);
            List<Map<String, Object>> existingReportDetails = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    Constants.EHRMS_USER_SYNC_TABLE, keyMap, null);

            if (!CollectionUtils.isEmpty(existingReportDetails)) {
                String status = (String) existingReportDetails.get(0).get(Constants.STATUS);
                if (Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase(status)) {
                    response.getParams().setStatus(Constants.SUCCESS);
                    response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                    response.setResponseCode(HttpStatus.OK);
                    return response;
                } else {
                    logger.info("Update BP report details::started");
                    return insertSyncDetailsInDBAndTriggerKafkaEvent(finalFromDate, finalToDate);
                }
            } else {
                logger.info("Insert BP report details into DB::started");
                return insertSyncDetailsInDBAndTriggerKafkaEvent(finalFromDate, finalToDate);
            }
        } catch (Exception e) {
            logger.error("Error while processing the request", e);
            updateErrorDetails(response, "Error while processing the request", HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }

    private void updateErrorDetails(SBApiResponse response, String errMsg, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(responseCode);
    }

    private SBApiResponse insertSyncDetailsInDBAndTriggerKafkaEvent(String fromDate, String toDate) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        try {
            Map<String, Object> dbRequest = new HashMap<>();
            dbRequest.put("job_name", "EHRMS_SYNC");
            dbRequest.put("job_id", UUID.randomUUID());
            dbRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            dbRequest.put(Constants.CREATED_DATE, new Date());
            dbRequest.put("from_date", fromDate);
            dbRequest.put("to_date", toDate);
            SBApiResponse dbResponse = cassandraOperation.insertRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, dbRequest);

            if (dbResponse.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                kafkaProducer.push(ehrmsUserDataSyncKafkaTopic, dbRequest);
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

    @Override
    public SBApiResponse getSyncStatus(Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_ENROLLMENT_BP_REPORT_STATUS);

        try {
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put("job_name", "EHRMS_SYNC");
            propertyMap.put(Constants.REPORT_REQUESTER, request.get(Constants.REPORT_REQUESTER));
            List<Map<String, Object>> reportList = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.EHRMS_USER_SYNC_TABLE, propertyMap, null);
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
}
