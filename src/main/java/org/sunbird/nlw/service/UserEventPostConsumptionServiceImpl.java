package org.sunbird.nlw.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
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
public class UserEventPostConsumptionServiceImpl implements UserEventPostConsumptionService {

    @Autowired
    CbExtServerProperties serverProperties;
    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    Producer producer;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    Logger logger = LogManager.getLogger(CQFConsumer.class);


    private boolean pushTokafkaEnabled = true;

    @Override
    public SBApiResponse processEventUsersForCertificateAndKarmaPoints(MultipartFile multipartFile) {
        List<String> headers;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(multipartFile.getInputStream(), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(serverProperties.getCsvDelimiter()).withFirstRecordAsHeader())) {

            headers = new ArrayList<>(csvParser.getHeaderNames());
            cleanHeaders(headers);
            try (CSVParser csvParser2 = new CSVParser(new BufferedReader(new InputStreamReader(multipartFile.getInputStream(), StandardCharsets.UTF_8)),
                    CSVFormat.newFormat(serverProperties.getCsvDelimiter()).withFirstRecordAsHeader())) {
                for (CSVRecord record : csvParser2.getRecords()) {

                    processRecord(record);
                }
            } catch (IOException e) {
                logger.error("Error processing CSV records", e);

            }

        } catch (IOException e) {
            logger.error("Error reading CSV file", e);
        }
        return null;
    }

    private void processRecord(CSVRecord record) throws IOException {
        String userid = record.get("userid");
        String contentid = record.get("contentid");
        String batchid = record.get("batchid");
        String lrcProgressdetails = record.get("lrc_progressdetails");
        Map<String, Object> lrcProgressdetailsMao = objectMapper.convertValue(lrcProgressdetails, new TypeReference<Map<String, Object>>() {
        });
        long duration = (long) lrcProgressdetailsMao.get("duration");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSXXX");
        LocalDateTime localDateTime = LocalDateTime.parse(record.get("completedon"), formatter);
        Date completedon = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
        if (duration >= 180) {
            generateKarmaPointEventAndPushToKafka(userid, contentid, batchid, completedon);
            String eventJson = generateIssueCertificateEvent(batchid, contentid, Arrays.asList(userid), 100.0, userid, completedon);
            if (pushTokafkaEnabled) {
                String topic = serverProperties.getUserIssueCertificateForEventTopic();
                kafkaTemplate.send(topic, userid, eventJson);
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

    public String generateIssueCertificateEvent(String batchId, String eventId, List<String> userIds, double eventCompletionPercentage, String userId, Date completedon) throws JsonProcessingException {
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
        return objectMapper.writeValueAsString(event);
    }
}
