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
    public SBApiResponse userEhrmsDataUpdate(Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.EHRMS_IGOT_USER_DATA_SYNC_API);

        try {
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            if (CollectionUtils.isEmpty(request)) {
                updateErrorDetails(response, "Request body is missing", HttpStatus.BAD_REQUEST);
            }

            if (!request.containsKey(Constants.FROM_DATE_CAMEL) || !request.containsKey(Constants.TO_DATE_CAMEL)) {
                updateErrorDetails(response, "fromDate and toDate are required", HttpStatus.BAD_REQUEST);
            }

            if (!(request.get(Constants.FROM_DATE_CAMEL) instanceof String) || !(request.get(Constants.TO_DATE_CAMEL) instanceof String)) {
                updateErrorDetails(response, "fromDate and toDate must be String in format dd-MM-yyyy", HttpStatus.BAD_REQUEST);
            }

            String fromDateStr = request.get(Constants.FROM_DATE_CAMEL).toString().trim();
            String toDateStr = request.get(Constants.TO_DATE_CAMEL).toString().trim();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-uuuu").withResolverStyle(ResolverStyle.STRICT);

            LocalDate from = null;
            LocalDate to = null;

            try {
                from = LocalDate.parse(fromDateStr, formatter);
                to = LocalDate.parse(toDateStr, formatter);
            } catch (DateTimeParseException e) {
                updateErrorDetails(response, "Invalid date format. Expected dd-MM-yyyy", HttpStatus.BAD_REQUEST);
            }

            if (to.isBefore(from)) {
                updateErrorDetails(response, "toDate cannot be before fromDate", HttpStatus.BAD_REQUEST);
            }

            if (to.isAfter(from.plusMonths(6))) {
                throw new IllegalArgumentException("Date range cannot exceed 6 months");
            }

            String finalFromDate = from.format(formatter);
            String finalToDate = to.format(formatter);

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put(Constants.JOB_NAME, Constants.EHRMS_SYNC);
            List<Map<String, Object>> existingJobs = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD, Constants.EHRMS_USER_SYNC_TABLE, keyMap, null);

            if (!CollectionUtils.isEmpty(existingJobs) && Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase((String) existingJobs.get(0).get(Constants.STATUS))) {
                Map<String, Object> lastJob = existingJobs.get(0);
                response.getParams().setStatus(Constants.SUCCESS);
                response.getResult().put(Constants.MESSAGE, "EHRMS sync already in progress");
                response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            insertSyncDetailsInDBAndTriggerKafkaEvent(finalFromDate, finalToDate);

            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put(Constants.MESSAGE, "EHRMS data sync started");
            response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            response.setResponseCode(HttpStatus.OK);
            return response;

        } catch (IllegalArgumentException e) {
            logger.warn("Validation failed", e);
            updateErrorDetails(response, e.getMessage(), HttpStatus.BAD_REQUEST);
            return response;

        } catch (Exception e) {
            logger.error("Error while processing EHRMS sync request", e);
            updateErrorDetails(response, "Internal server error", HttpStatus.INTERNAL_SERVER_ERROR);
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
            dbRequest.put(Constants.JOB_NAME, Constants.EHRMS_SYNC);
            dbRequest.put(Constants.JOB_START_DATE, new Date());
            dbRequest.put(Constants.JOB_ID, UUID.randomUUID().toString());
            dbRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            dbRequest.put(Constants.EHRMS_FROM_DATE, fromDate);
            dbRequest.put(Constants.EHRMS_TO_DATE, toDate);
            SBApiResponse dbResponse = cassandraOperation.insertRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.EHRMS_USER_SYNC_TABLE, dbRequest);

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
    public SBApiResponse getSyncStatus() {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_USER_ENROLLMENT_BP_REPORT_STATUS);

        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.JOB_NAME, Constants.EHRMS_SYNC);
            List<Map<String, Object>> reportList = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.EHRMS_USER_SYNC_TABLE, propertyMap, null);
            if (CollectionUtils.isEmpty(reportList)) {
                updateErrorDetails(response, "EHRMS SYNC status not available", HttpStatus.OK);
                return response;
            } else {
                response.getParams().setStatus(Constants.SUCCESSFUL);
                response.setResponseCode(HttpStatus.OK);
                response.getResult().put(Constants.CONTENT, reportList.get(0));
                response.getResult().put(Constants.COUNT, reportList.size());
            }

        } catch (Exception e) {
            setErrorData(response, String.format("Failed to get EHRMS SYNC status. Error: ", e.getMessage()));
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
        return ResponseEntity.status(status).headers(headers).body(new ByteArrayResource(message.getBytes()));
    }
}
