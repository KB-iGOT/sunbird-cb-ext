package org.sunbird.nlw.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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

    private Map<String, Object> eventInfoCache = new HashMap<>();

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
        eventInfoCache.clear();
        return response;
    }

    @Override
    public SBApiResponse processEventUsersForStatus(MultipartFile multipartFile) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.USER_EVENT_CONSUMPTION);
        List<String> headers;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(multipartFile.getInputStream(), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(serverProperties.getCsvDelimiter()).withFirstRecordAsHeader())) {
             headers = new ArrayList<>(csvParser.getHeaderNames());
             cleanHeaders(headers);
                for (CSVRecord record : csvParser.getRecords()) {
                    processRecordForStatus(record);
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
        eventInfoCache.clear();
        return response;
    }

    private void processRecord(CSVRecord record) throws IOException {
        String userid = record.get("userid");
        String contentid = record.get("contentid");
        String batchid = record.get("batchid");
        logger.info(String.format("Processing User event enrolment. UserId: %s, EventId: %s, BatchId: %s", userid, contentid, batchid));
        List<Map<String, Object>> enrolmentRecords = fetchEnrolmentRecordsForUser(userid, contentid, batchid);
        if (!enrolmentRecords.isEmpty()) {
            logger.info("User event enrolment found.");
            Map<String, Object> enrolmentRecord = enrolmentRecords.get(0);
            if (((int) enrolmentRecord.get("status") == 0) || enrolmentRecord.get("lrc_progressdetails") == null) {
                logger.info("Status is 0 or progressDetails not found. Skipping record.");
                return;
            }
            Map<String, Object> updateEnrollmentRecords = prepareUpdatedEnrollmentRecord(enrolmentRecord);
            if (updateEnrollmentRecords != null) {
                Date completedon = (Date) updateEnrollmentRecords.get("completedon");
                Map<String, Object> keyMap = new HashMap<>();
                keyMap.put(Constants.USER_ID, userid);
                keyMap.put(Constants.CONTENT_ID_KEY, contentid);
                keyMap.put(Constants.CONTEXT_ID_CAMEL, contentid);
                keyMap.put(Constants.BATCH_ID, batchid);
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME,
                        serverProperties.getUserEventEnrolmentTable(), updateEnrollmentRecords, keyMap);
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                    logger.info("Successfully updated DB");
                    if (enrolmentRecord.get("issuedCertificates") == null || ((List<?>)enrolmentRecord.get("issuedCertificates")).size() == 0) {
                        generateIssueCertificateEvent(batchid, contentid, Arrays.asList(userid), 100.0, userid,
                                completedon);
                    }
                    generateKarmaPointEventAndPushToKafka(userid, contentid, batchid, completedon);
                } else {
                    logger.error("Failed to update records with updated details");
                }
            } else {
                logger.error("Failed to compute completedOn value.");
            }
        } else {
            logger.info("User event enrolment not found.");
        }
    }

    private void cleanHeaders(List<String> headers) {
        headers.replaceAll(header -> header.replaceAll("^\"|\"$", ""));
    }

    public void generateKarmaPointEventAndPushToKafka(String userId, String eventId, String batchId, Date completedon) {
        long ets = completedon.getTime() - 10 * 1000;
        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("user_id", userId);
        objectMap.put("ets", ets);
        objectMap.put("event_id", eventId);
        objectMap.put("batch_id", batchId);
        producer.pushWithKey(serverProperties.getUserEventKarmaPointTopic(), objectMap, userId);
        logger.info("Pushed kafka message for issue-karma-points");
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
        producer.pushWithKey(serverProperties.getUserIssueCertificateForEventTopic(), event, userId);
        logger.info("Pushed kafka message for issue-event-certificate");
    }

    private List<Map<String, Object>> fetchEnrolmentRecordsForUser(String userId, String eventId, String batchId) {

        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.USER_ID, userId);
        compositeKey.put(Constants.CONTENT_ID_KEY, eventId);
        compositeKey.put(Constants.CONTEXT_ID_CAMEL, eventId);
        compositeKey.put(Constants.BATCH_ID, batchId);

        List<Map<String, Object>> enrolmentRecords = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, serverProperties.getUserEventEnrolmentTable(), compositeKey, null);
        return enrolmentRecords;
    }

    private Map<String, Object> prepareUpdatedEnrollmentRecord(Map<String, Object> enrolmentRecord) throws IOException {
        String contentId = (String) enrolmentRecord.get("contentId");
        String batchId = (String) enrolmentRecord.get("batchId");
        String cacheKey = contentId + "-" + batchId;
        Integer eventDuration = 0;
        Date eventUpdatedEndDate = null;
        if (eventInfoCache.containsKey(cacheKey)) { 
            Map<String, Object> eventInfo = (Map<String, Object>) eventInfoCache.get(cacheKey);
            eventDuration = (Integer) eventInfo.get("duration");
            eventUpdatedEndDate = (Date) eventInfo.get("completedon");
        } else {
            Map<String, Object> keyMap = new HashMap<>();
            keyMap.put("eventid", contentId);
            keyMap.put("batchid", batchId);
            List<Map<String, Object>> eventData = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.EVENT_BATCH_TABLE_NAME, keyMap,
                    Arrays.asList("end_date", "batch_attributes"));
            if (!eventData.isEmpty()) {
                try {
                    Date endDateAsDate = (Date) eventData.get(0).get("end_date");
                    ZonedDateTime endDate = endDateAsDate.toInstant().atZone(ZoneOffset.UTC);

                    JsonNode batchAttributesJson = (new ObjectMapper())
                            .readTree((String) eventData.get(0).get("batch_attributes"));
                    String endTimeStr = batchAttributesJson.get("endTime").asText();

                    // Parse endTime with offset as OffsetTime, then reduce it by 1 minute
                    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ssXXX");
                    OffsetTime endTime = OffsetTime.parse(endTimeStr, timeFormatter).minusMinutes(1);

                    // Combine date from end_date and adjusted time from batch_attributes.endTime
                    ZonedDateTime endDateTime = endDate.withHour(endTime.getHour())
                            .withMinute(endTime.getMinute())
                            .withSecond(endTime.getSecond())
                            .withNano(endTime.getNano());

                    eventUpdatedEndDate = Date.from(endDateTime.toInstant());
                    // Get Duration from batchAttributes column - duration value which is in
                    // minutes.
                    eventDuration = 60 * batchAttributesJson.get("duration").asInt();
                    Map<String, Object> eventInfo = new HashMap<String, Object>();
                    eventInfo.put("completedon", eventUpdatedEndDate);
                    eventInfo.put("duration", eventDuration);
                    eventInfoCache.put(cacheKey, eventInfo);
                } catch (Exception e) {
                    logger.error("Failed to parse the end_date details. Exception: ", e);
                    return null;
                }
            } else {
                logger.error("No matching event_batch record found for the specified eventid and batchid.");
                return null;
            }
        }

        Map<String, Object> updatedRecord = new HashMap<>();
        updatedRecord.put("completionpercentage", 100.0f);
        updatedRecord.put("progress", 100);
        updatedRecord.put("status", 2);
        updatedRecord.put("completedon", eventUpdatedEndDate);

        String lrcProgressdetailsJson = (String) enrolmentRecord.get("lrc_progressdetails");
        Map<String, Object> lrcProgressdetailsMap = objectMapper.readValue(lrcProgressdetailsJson,
                new TypeReference<Map<String, Object>>() {
                });
        // Check existing max_size is String or integer
        Integer maxSize = 0;
        if (lrcProgressdetailsMap.get("max_size") instanceof String) {
            maxSize = (int) Double.parseDouble((String) lrcProgressdetailsMap.get("max_size"));
        } else if (lrcProgressdetailsMap.get("max_size") instanceof Double) {
            maxSize = (int) ((double) lrcProgressdetailsMap.get("max_size"));
        } else {
            maxSize = (int) lrcProgressdetailsMap.get("max_size");
        }

        Integer duration = 0;
        if (lrcProgressdetailsMap.get("duration") instanceof Double) {
            duration = (int) ((double) lrcProgressdetailsMap.get("duration"));
        } else {
            duration = (int) lrcProgressdetailsMap.get("duration");
        }
        Integer status = (Integer) enrolmentRecord.get("status");
        // is this maxSize is minutes or seconds ?
        if (!eventDuration.equals(maxSize)) {
            // this is in minutes... convert duration to seconds
            duration = 60 * duration;
        }

        if (status == 0 || (status == 1 && duration < 180)) {
            // We don't need to update anything... just return null;
            logger.info("Skipping record. Status is 0 OR Event consumption duration is less than 180 seconds.");
            return null;
        } else if (status == 2) {
            if (enrolmentRecord.get("issuedCertificates") == null || ((List<?>)enrolmentRecord.get("issuedCertificates")).size() == 0) {
                logger.error("Skipping malformed user record. Status is 2 but cert is not issued.");
                return null;
            }
        }

        lrcProgressdetailsMap.put("max_size", eventDuration);
        lrcProgressdetailsMap.put("duration", eventDuration);
        lrcProgressdetailsMap.put("stateMetaData", eventDuration);
        updatedRecord.put("lrc_progressdetails", objectMapper.writeValueAsString(lrcProgressdetailsMap));

        logger.info("Updated completedOn with event end_date: " + eventUpdatedEndDate);

        return updatedRecord;
    }

    private void processRecordForStatus(CSVRecord record) throws IOException {
        String userid = record.get("userid");
        String contentid = record.get("contentid");
        String batchid = record.get("batchid");
        logger.info(String.format("Processing User event enrolment. UserId: %s, EventId: %s, BatchId: %s", userid, contentid, batchid));
        List<Map<String, Object>> enrolmentRecords = fetchEnrolmentRecordsForUser(userid, contentid, batchid);
        if (!enrolmentRecords.isEmpty()) {
            logger.info("User event enrolment found.");
            Map<String, Object> enrolmentRecord = enrolmentRecords.get(0);
            int status = (int) enrolmentRecord.get("status");
            if (status == 2 && enrolmentRecord.get("issuedCertificate") == null || ((List<?>)enrolmentRecord.get("issuedCertificates")).size() == 0) {
                //Status is 2 -- but no certificate... setting this back to 1
                Map<String, Object> updateEnrollmentRecords = new HashMap<String, Object>();
                updateEnrollmentRecords.put("status", 1);
                Map<String, Object> keyMap = new HashMap<>();
                keyMap.put(Constants.USER_ID, userid);
                keyMap.put(Constants.CONTENT_ID_KEY, contentid);
                keyMap.put(Constants.CONTEXT_ID_CAMEL, contentid);
                keyMap.put(Constants.BATCH_ID, batchid);
                Map<String, Object> resp = cassandraOperation.updateRecord(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME,
                        serverProperties.getUserEventEnrolmentTable(), updateEnrollmentRecords, keyMap);
                if (resp.get(Constants.RESPONSE).equals(Constants.SUCCESS)) {
                    logger.info("Successfully updated status to 1"); 
                } else {
                    logger.error("Failed to update status to 1");
                }
            } else {
                logger.info("Skipping record - doesn't match criteria.");
            }
        } else {
            logger.info("User event enrolment not found.");
        }
    }
}
