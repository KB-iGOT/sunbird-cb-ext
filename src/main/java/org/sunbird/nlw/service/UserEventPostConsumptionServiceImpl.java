package org.sunbird.nlw.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jnr.ffi.annotations.In;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.consumer.CQFConsumer;
import org.sunbird.core.producer.Producer;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author mahesh.vakkund
 */
@Service
public class UserEventPostConsumptionServiceImpl implements UserEventPostConsumptionService {

    @Autowired
    CbExtServerProperties serverProperties;
    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    Producer producer;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    CassandraOperation cassandraOperation;

    Logger logger = LogManager.getLogger(CQFConsumer.class);


    private boolean pushTokafkaEnabled = true;

    private Map<String, Date> endDateCache = new HashMap<>();

    @Override
    public SBApiResponse processEventUsersForCertificateAndKarmaPoints(MultipartFile multipartFile) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.USER_EVENT_CONSUMPTION);
        List<String> headers;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(multipartFile.getInputStream(), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(serverProperties.getCsvDelimiter()).withFirstRecordAsHeader())) {
             headers = new ArrayList<>(csvParser.getHeaderNames());
             cleanHeaders(headers);
                for (CSVRecord record : csvParser.getRecords()) {
                    processRecord(record);
                }
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.MESSAGE, "File processed successfully");
        } catch (IOException e) {
            logger.error("Error reading CSV file", e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrmsg("Failed to process the file: " + e.getMessage());
        } catch (Exception e) {
            logger.error("An unexpected error occurred", e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrmsg("An unexpected error occurred: " + e.getMessage());
        }
        return response;
    }

    private void processRecord(CSVRecord record) throws IOException {
        String userid = record.get("userid");
        String contentid = record.get("contentid");
        String batchid = record.get("batchid");
        List<Map<String, Object>> enrolmentRecords = fetchEnrolmentRecordsForUser(userid, contentid, batchid);
        if (!enrolmentRecords.isEmpty()) {
            Map<String, Object> enrolmentRecord = enrolmentRecords.get(0);
            String lrcProgressdetails = (String) enrolmentRecord.get("lrc_progressdetails");
            JsonNode lrcProgressdetailsMap = objectMapper.readTree(lrcProgressdetails);
            long duration = lrcProgressdetailsMap.get("duration").asLong();
            if (duration >= 180) {
                Map<String,Object> updateEnrollmentRecords = prepareUpdatedEnrollmentRecord(enrolmentRecord);
                Date completedon = (Date) updateEnrollmentRecords.get("completedon");
                Map<String,Object> keyMap = new HashMap<>();
                keyMap.put(Constants.USER_ID, userid);
                keyMap.put(Constants.CONTENT_ID_KEY, contentid);
                keyMap.put(Constants.CONTEXT_ID_CAMEL, contentid);
                keyMap.put(Constants.BATCH_ID, batchid);
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, serverProperties.getUserEventEnrolmentTable(),updateEnrollmentRecords,keyMap);
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                    generateIssueCertificateEvent(batchid,contentid, Arrays.asList(userid), 100.0, userid, completedon);
                    generateKarmaPointEventAndPushToKafka(userid, contentid, batchid, completedon);
                } else {
                    logger.info("failed to update records with updated details");
                }
            }
        }
    }

    private void cleanHeaders(List<String> headers) {
        headers.replaceAll(header -> header.replaceAll("^\"|\"$", ""));
    }

    public void generateKarmaPointEventAndPushToKafka(String userId, String eventId, String batchId, Date completedon) {
        long ets = completedon.getTime() - 10 * 1000;
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("user_id", userId);
        objectMap.put("etc", ets);
        objectMap.put("event_id", eventId);
        objectMap.put("batch_id", batchId);
        producer.pushWithKey(serverProperties.getUserEventKarmaPointTopic(), objectMap, userId);

    }

    public void generateIssueCertificateEvent(String batchId, String eventId, List<String> userIds, double eventCompletionPercentage, String userId, Date completedon) throws JsonProcessingException {
        long ets = completedon.getTime() - 10 * 1000;
        // Generate a UUID for the message ID
        String mid = UUID.randomUUID().toString();
        Map<String, Object> event = new HashMap<>();
        Map<String, Object> actor = new HashMap<>();
        actor.put("id", "Issue Certificate Generator");
        actor.put("type", "System");
        event.put("actor", actor);
        Map<String, Object> context = new HashMap<>();
        JSONObject pdata = new JSONObject();
        pdata.put("version", "1.0");
        pdata.put("id", "org.sunbird.learning.platform");
        context.put("pdata", pdata);
        event.put("context", context);

        Map<String, Object> edata = new HashMap<>();
        edata.put("action", "issue-event-certificate");
        edata.put("eventType", "offline"); // Add mode here
        edata.put("batchId", batchId);
        edata.put("eventId", eventId);
        edata.put("userIds", userIds);
        edata.put("eventCompletionPercentage", eventCompletionPercentage);
        event.put("edata", edata);

        event.put("eid", "BE_JOB_REQUEST");
        event.put("ets", ets);
        event.put("mid", mid);

        Map<String, Object> object = new HashMap<>();
        object.put("id", userId);
        object.put("type", "IssueCertificate");
        event.put("object", object);
        producer.push(serverProperties.getUserIssueCertificateForEventTopic(),event);
    }

    private List<Map<String, Object>> fetchEnrolmentRecordsForUser(String userId, String eventId, String batchId) {

        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.USER_ID, userId);
        compositeKey.put(Constants.CONTENT_ID_KEY, eventId);
        compositeKey.put(Constants.CONTEXT_ID_CAMEL, eventId);
        compositeKey.put(Constants.BATCH_ID, batchId);

        List<Map<String, Object>> enrolmentRecords = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, serverProperties.getUserEventEnrolmentTable(), compositeKey, null);
        return enrolmentRecords;
    }

    private Map<String, Object> prepareUpdatedEnrollmentRecord(Map<String, Object> enrolmentRecord) throws IOException {

            String lrcProgressdetailsJson = (String) enrolmentRecord.get("lrc_progressdetails");
            Map<String, Object> lrcProgressdetailsMap = objectMapper.readValue(lrcProgressdetailsJson, new TypeReference<Map<String, Object>>() {});

            Integer maxSize = (Integer) lrcProgressdetailsMap.get("max_size");

            lrcProgressdetailsMap.put("duration", maxSize);
            lrcProgressdetailsMap.put("stateMetaData", maxSize);

            Map<String, Object> updatedRecord = new HashMap<>();
            updatedRecord.put("lrc_progressdetails", objectMapper.writeValueAsString(lrcProgressdetailsMap));
            Date completedOn = (Date) enrolmentRecord.get("completedon");
            if (completedOn == null) {
                String contentId = (String) enrolmentRecord.get("contentId");
                String batchId = (String) enrolmentRecord.get("batchId");
                String cacheKey = contentId + "-" + batchId;
                if (endDateCache.containsKey(cacheKey)) {
                    completedOn = endDateCache.get(cacheKey);
                    updatedRecord.put("completedon", completedOn);
                } else {
                    Map<String, Object> keyMap = new HashMap<>();
                    keyMap.put("eventid", contentId);
                    keyMap.put("batchid", batchId);
                    List<Map<String, Object>> eventData = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.EVENT_BATCH_TABLE_NAME, keyMap, Arrays.asList("end_date"));
                    if (!eventData.isEmpty()) {
                        completedOn = (Date) eventData.get(0).get("end_date");
                        endDateCache.put(cacheKey, completedOn);
                        if (completedOn != null) {
                            updatedRecord.put("completedon", completedOn);
                            logger.info("Updated completedOn with event end_date: " + completedOn);
                        } else {
                            logger.info("End date is null in the event_batch table record.");
                        }
                    } else {
                        logger.info("No matching event_batch record found for the specified eventid and batchid.");
                    }
                }
            }
            updatedRecord.put("status", 2);
            updatedRecord.put("completionpercentage", 100.0f);
            updatedRecord.put("progress", 100);
            logger.info("Successfully prepared the updated fields");
            return updatedRecord;
    }
}
