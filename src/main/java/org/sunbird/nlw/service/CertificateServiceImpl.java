package org.sunbird.nlw.service;

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

    public void generateCertificateEventAndPushToKafka(String userId, String eventId, String batchId, double completionPercentage) throws IOException {
        List<String> userIds = Collections.singletonList(userId);
        String eventJson = generateIssueCertificateEvent(batchId, eventId, userIds, completionPercentage, userId);
        if (pushTokafkaEnabled) {
            String topic = serverProperties.getUserIssueCertificateForEventTopic();
            kafkaTemplate.send(topic, userId, eventJson);
        }
    }

    public static String generateIssueCertificateEvent(String batchId, String eventId, List<String> userIds, double eventCompletionPercentage, String userId) {
        // Generate current timestamp (in milliseconds)
        long ets = System.currentTimeMillis();

        // Generate a UUID for the message ID
        String mid = UUID.randomUUID().toString();

        // Create the event object as a JSONObject
        JSONObject event = new JSONObject();

        // Construct "actor"
        JSONObject actor = new JSONObject();
        actor.put("id", "Issue Certificate Generator");
        actor.put("type", "System");
        event.put("actor", actor);

        // Construct "context"
        JSONObject context = new JSONObject();
        JSONObject pdata = new JSONObject();
        pdata.put("version", "1.0");
        pdata.put("id", "org.sunbird.learning.platform");
        context.put("pdata", pdata);
        event.put("context", context);

        // Construct "edata"
        JSONObject edata = new JSONObject();
        edata.put("action", "issue-event-certificate");
        edata.put("eventType", "offline"); // Add mode here
        edata.put("batchId", batchId);
        edata.put("eventId", eventId);
        edata.put("userIds", userIds);
        edata.put("eventCompletionPercentage", eventCompletionPercentage);
        event.put("edata", edata);

        // Add other attributes
        event.put("eid", "BE_JOB_REQUEST");
        event.put("ets", ets);
        event.put("mid", mid);

        // Construct "object"
        JSONObject object = new JSONObject();
        object.put("id", userId);
        object.put("type", "IssueCertificate");
        event.put("object", object);

        // Return the event as a JSON string
        return event.toString();
    }
}