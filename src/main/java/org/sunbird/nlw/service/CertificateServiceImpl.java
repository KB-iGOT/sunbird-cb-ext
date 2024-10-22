package org.sunbird.nlw.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.core.producer.Producer;

import java.io.IOException;
import java.util.*;

@Component
public class CertificateServiceImpl {

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    ObjectMapper objectMapper = new ObjectMapper();

    private boolean pushTokafkaEnabled = true;

    public void generateCertificateEventAndPushToKafka(String userId, String eventId, String batchId, double completionPercentage) throws IOException {
        long now = System.currentTimeMillis();
        List<String> userIds = Collections.singletonList(userId);

        // Create the event JSON string
        String event = String.format(
                "{ \"actor\": { \"id\": \"Issue Certificate Generator\", \"type\": \"System\" }, " +
                        "\"context\": { \"pdata\": { \"version\": \"1.0\", \"id\": \"org.sunbird.learning.platform\" } }, " +
                        "\"edata\": { \"action\": \"issue-event-certificate\", \"eventType\": \"offline\", \"batchId\": \"%s\", " +
                        "\"eventId\": \"%s\", \"userIds\": \"%s\", \"eventCompletionPercentage\": %.2f }, " +
                        "\"eid\": \"BE_JOB_REQUEST\", " +
                        "\"ets\": %d, " +
                        "\"mid\": \"EVENT.%s\", " +
                        "\"object\": { \"id\": \"%s\", \"type\": \"IssueCertificate\" } }",
                batchId, eventId, userIds.toString(), completionPercentage, now, UUID.randomUUID(), userId
        ).replaceAll("\n", "");

        // If pushing to Kafka is enabled, send the message
        if (pushTokafkaEnabled) {
            String topic = serverProperties.getUserIssueCertificateForEventTopic();
            kafkaTemplate.send(topic, userId, event);
        }
    }
}
