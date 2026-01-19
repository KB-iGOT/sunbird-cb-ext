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
import org.springframework.util.ObjectUtils;
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
    public SBApiResponse userEhrmsDataUpdate(Map<String, Object> requestBody, String sync) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.EHRMS_IGOT_USER_DATA_SYNC_API);

        try {
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            boolean isSync = Boolean.parseBoolean(Optional.ofNullable(sync).orElse("false"));
            if (CollectionUtils.isEmpty(request)) {
                updateErrorDetails(response, "Request body is missing", HttpStatus.BAD_REQUEST);
                return response;
            }

            String fromDateStr;
            String toDateStr;
            String jobType = null;

            DateTimeFormatter apiFormatter = DateTimeFormatter.ofPattern("dd-MM-uuuu").withResolverStyle(ResolverStyle.STRICT);
            DateTimeFormatter internalFormatter = DateTimeFormatter.ofPattern("dd-MM-uuuu");
            LocalDate today = LocalDate.now();

            Object jobTypeObj = request.get(Constants.JOB_TYPE_CAMEL);

            if (!ObjectUtils.isEmpty(jobTypeObj)) {
                jobType = jobTypeObj.toString();
                if (Constants.DAILY.equalsIgnoreCase(jobType)) {
                    fromDateStr = today.minusDays(1).format(internalFormatter);
                    toDateStr = today.format(internalFormatter);
                } else if (Constants.WEEKLY.equalsIgnoreCase(jobType)) {
                    fromDateStr = today.minusDays(7).format(internalFormatter);
                    toDateStr = today.format(internalFormatter);
                } else if (Constants.MONTHLY.equalsIgnoreCase(jobType)) {
                    fromDateStr = today.minusDays(30).format(internalFormatter);
                    toDateStr = today.format(internalFormatter);
                } else if (Constants.HALF_YEARLY.equalsIgnoreCase(jobType)) {
                    fromDateStr = today.minusDays(180).format(internalFormatter);
                    toDateStr = today.format(internalFormatter);
                } else {
                    updateErrorDetails(response, "Invalid jobType. Allowed values: DAILY, WEEKLY, MONTHLY, HALFYEARLY",
                            HttpStatus.BAD_REQUEST);
                    return response;
                }

            } else {

                Object fromObj = request.get(Constants.FROM_DATE_CAMEL);
                Object toObj = request.get(Constants.TO_DATE_CAMEL);
                if (ObjectUtils.isEmpty(fromObj) || ObjectUtils.isEmpty(toObj)) {
                    updateErrorDetails(response, "Either jobType or fromDate & toDate must be provided", HttpStatus.BAD_REQUEST);
                    return response;
                }
                fromDateStr = fromObj.toString();
                toDateStr = toObj.toString();
            }

            LocalDate from;
            LocalDate to;

            try {
                from = LocalDate.parse(fromDateStr, apiFormatter);
                to = LocalDate.parse(toDateStr, apiFormatter);
            } catch (DateTimeParseException e) {
                updateErrorDetails(response, "Invalid date format. Expected dd-MM-yyyy", HttpStatus.BAD_REQUEST);
                return response;
            }

            //6 month validation only for manual range
            if (ObjectUtils.isEmpty(jobType)) {
                if (to.isBefore(from)) {
                    updateErrorDetails(response, "toDate cannot be before fromDate", HttpStatus.BAD_REQUEST);
                    return response;
                }
                if (to.isAfter(from.plusMonths(6))) {
                    updateErrorDetails(response, "Date range cannot exceed 6 months", HttpStatus.BAD_REQUEST);
                    return response;
                }
            }

            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put(Constants.JOB_NAME, Constants.EHRMS_SYNC);
            List<Map<String, Object>> existingJobs =
                    cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                            Constants.EHRMS_USER_SYNC_TABLE, keyMap, null);

            if (!CollectionUtils.isEmpty(existingJobs)
                    && Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase(
                    (String) existingJobs.get(0).get(Constants.STATUS))) {

                response.getParams().setStatus(Constants.SUCCESS);
                response.getResult().put(Constants.MESSAGE, "EHRMS sync already in progress");
                response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            insertSyncDetailsInDBAndTriggerKafkaEvent(fromDateStr, toDateStr, isSync, jobType);

            response.getParams().setStatus(Constants.SUCCESS);
            response.getResult().put(Constants.MESSAGE, "EHRMS data sync started");
            response.getResult().put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            response.setResponseCode(HttpStatus.OK);
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

    private SBApiResponse insertSyncDetailsInDBAndTriggerKafkaEvent(String fromDate, String toDate, boolean isSync, String jobType) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.BP_REPORT_GENERATE_API);
        try {
            Map<String, Object> dbRequest = new HashMap<>();
            dbRequest.put(Constants.JOB_NAME, Constants.EHRMS_SYNC);
            dbRequest.put(Constants.JOB_START_DATE, new Date());
            dbRequest.put(Constants.JOB_ID, UUID.randomUUID().toString());
            dbRequest.put(Constants.STATUS, Constants.STATUS_IN_PROGRESS_UPPERCASE);
            dbRequest.put(Constants.EHRMS_FROM_DATE, fromDate);
            dbRequest.put(Constants.EHRMS_TO_DATE, toDate);
            dbRequest.put(Constants.IS_SYNC, isSync);
            dbRequest.put(Constants.JOB_TYPE_COLUMN, jobType);
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
