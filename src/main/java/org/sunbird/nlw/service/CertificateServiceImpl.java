package org.sunbird.nlw.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;

import java.io.IOException;
import java.util.*;

@Component
public class CertificateServiceImpl {

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    private boolean pushTokafkaEnabled = true;

    ObjectMapper mapper = new ObjectMapper();

    public void generateCertificateEventAndPushToKafka(String userId, String eventId, String batchId, double completionPercentage) throws IOException {
        List<String> userIds = Collections.singletonList(userId);
        String eventJson = generateIssueCertificateEvent(batchId, eventId, userIds, completionPercentage, userId);
        if (pushTokafkaEnabled) {
            String topic = serverProperties.getUserIssueCertificateForEventTopic();
            kafkaTemplate.send(topic, userId, eventJson);
        }
    }

    public String generateIssueCertificateEvent(String batchId, String eventId, List<String> userIds, double eventCompletionPercentage, String userId) throws JsonProcessingException {
        // Generate current timestamp (in milliseconds)
        long ets = System.currentTimeMillis();

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

        return mapper.writeValueAsString(event);
    }
}