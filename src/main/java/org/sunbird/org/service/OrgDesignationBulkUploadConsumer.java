package org.sunbird.org.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
public class OrgDesignationBulkUploadConsumer {

    private final Logger logger = LoggerFactory.getLogger(OrgDesignationBulkUploadConsumer.class);

    @Autowired
    OrgDesignationMappingService orgDesignationMappingService;

    private final List<String> messageBuffer = Collections.synchronizedList(new ArrayList<>());


    @KafkaListener(topics = "${kafka.topics.org.designation.bulk.upload.event}", groupId = "${kafka.topics.org.designation.bulk.upload.event.group}")
    public void processOrgDesignationBulkUploadMessage(ConsumerRecord<String, String> data) {
        logger.info(
                "OrgDesignationBulkUploadMessage::processMessage: Received event to initiate Org Designation Bulk Upload Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                synchronized (messageBuffer) {
                    messageBuffer.add(data.value());
                }

                // Process the message asynchronously
                CompletableFuture.runAsync(() -> {
                    orgDesignationMappingService.initiateOrgDesignationBulkUploadProcess(data.value());

                    // Remove successfully processed message from buffer
                    synchronized (messageBuffer) {
                        messageBuffer.remove(data.value());
                    }
                });
            } else {
                logger.error("Error in Org Designation Bulk Upload Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Org Designation Bulk Upload Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }

    @PreDestroy
    public void shutdownHook() {
        logger.info("Shutdown hook triggered. Processing buffered messages...");
        synchronized (messageBuffer) {
            for (String message : messageBuffer) {
                try {
                    logger.info("Processing buffered message: {}", message);
                    orgDesignationMappingService.updateDBStatusAtShutDown(message);
                    logger.info("Successfully processed message during shutdown: {}", message);
                } catch (Exception e) {
                    logger.error("Error processing message during shutdown: {}", message, e);
                }
            }
            messageBuffer.clear(); // Clear buffer after processing
        }
        logger.info("Shutdown hook completed.");
    }
}
