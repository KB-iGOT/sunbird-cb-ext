package org.sunbird.org.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrgDesignationBulkUploadConsumer {

    private final Logger logger = LoggerFactory.getLogger(OrgDesignationBulkUploadConsumer.class);
    @Autowired
    OrgDesignationMappingService orgDesignationMappingService;


    @KafkaListener(topics = "${kafka.topics.org.designation.bulk.upload.event}", groupId = "${kafka.topics.org.designation.bulk.upload.event.group}")
    public void processOrgDesignationBulkUploadMessage(ConsumerRecord<String, String> data) {
        logger.info(
                "OrgDesignationBulkUploadMessage::processMessage: Received event to initiate Org Designation Bulk Upload Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    orgDesignationMappingService.initiateOrgDesignationBulkUploadProcess(data.value());
                });
            } else {
                logger.error("Error in Org Designation Bulk Upload Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Org Designation Bulk Upload Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }
}
