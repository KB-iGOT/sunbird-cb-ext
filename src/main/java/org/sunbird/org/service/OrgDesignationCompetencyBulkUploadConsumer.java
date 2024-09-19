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
public class OrgDesignationCompetencyBulkUploadConsumer {

    private final Logger logger = LoggerFactory.getLogger(OrgDesignationCompetencyBulkUploadConsumer.class);
    @Autowired
    OrgDesignationCompetencyMappingService orgDesignationCompetencyMappingService;


    @KafkaListener(topics = "${kafka.topics.competency.designation.bulk.upload.event}", groupId = "${kafka.topics.competency.designation.bulk.upload.event.group}")
    public void processOrgDesignationCompetencyBulkUploadMessage(ConsumerRecord<String, String> data) {
        logger.info(
                "OrgDesignationCompetencyBulkUploadMessage::processMessage: Received event to initiate Org Competency Designation Bulk Upload Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    orgDesignationCompetencyMappingService.initiateCompetencyDesignationBulkUploadProcess(data.value());
                });
            } else {
                logger.error("Error in Org Competency Designation Mapping Bulk Upload Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Competency Designation Bulk Upload Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }
}
