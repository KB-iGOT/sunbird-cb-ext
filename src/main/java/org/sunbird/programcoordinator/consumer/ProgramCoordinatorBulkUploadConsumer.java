package org.sunbird.programcoordinator.consumer;

import java.util.concurrent.CompletableFuture;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.programcoordinator.service.ProgramCoordinatorBulkUploadProcessingService;

/**
 * Kafka consumer for Program Coordinator bulk upload events. Mirrors
 * org.sunbird.nongovtuser.service.NonGovtUserBulkUploadConsumer: receives the event published by
 * ProgramCoordinatorBulkUploadServiceImpl.triggerBulkUploadKafkaEvent and hands it off to
 * ProgramCoordinatorBulkUploadProcessingService asynchronously.
 */
@RequiredArgsConstructor
@Component
public class ProgramCoordinatorBulkUploadConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ProgramCoordinatorBulkUploadConsumer.class);

    private final ProgramCoordinatorBulkUploadProcessingService programCoordinatorBulkUploadProcessingService;

    @KafkaListener(topics = "${kafka.topics.program.coordinator.bulk.upload}",
            groupId = "${kafka.topics.program.coordinator.bulk.upload.group}")
    public void processProgramCoordinatorBulkUploadMessage(ConsumerRecord<String, String> data) {
        if (StringUtils.isBlank(data.value())) {
            logger.error("ProgramCoordinatorBulkUploadConsumer:: processProgramCoordinatorBulkUploadMessage: Invalid Kafka Msg");
            return;
        }
        logger.info("ProgramCoordinatorBulkUploadConsumer:: processProgramCoordinatorBulkUploadMessage: "
                + "Received event to initiate Program Coordinator Bulk Upload Process, programId: {}", data.key());
        CompletableFuture.runAsync(() ->
                programCoordinatorBulkUploadProcessingService.initiateProgramCoordinatorBulkUploadProcess(data.value()));
    }
}
