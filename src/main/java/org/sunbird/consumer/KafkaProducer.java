package org.sunbird.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

@Component
public class KafkaProducer {
    private Logger logger = LoggerFactory.getLogger(getClass().getName());
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    public void push(String topic, Object data) {
        try {
            String message = objectMapper.writeValueAsString(data);
            logger.info("KafkaProducer::sendUserEmailData: topic: {}", topic);
            this.kafkaTemplate.send(topic, message);
            logger.info("Data sent to kafka topic {} and message is {}", topic, message);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }

    }

}
