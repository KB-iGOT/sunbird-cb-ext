package org.sunbird.nongovtuser.service;

import java.util.concurrent.CompletableFuture;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for Non-Govt (Volunteer) bulk user upload events.
 * Mirrors UserBulkUploadConsumer: receives the event published by
 * NonGovtUserBulkUploadServiceImpl.triggerBulkUploadKafkaEvent and hands it off to
 * NonGovtUserBulkUploadProcessingService asynchronously.
 */
@RequiredArgsConstructor
@Component
public class NonGovtUserBulkUploadConsumer {

    private static final Logger logger = LoggerFactory.getLogger(NonGovtUserBulkUploadConsumer.class);

    private final NonGovtUserBulkUploadProcessingService nonGovtUserBulkUploadProcessingService;

    @KafkaListener(topics = "${kafka.topics.nongovt.user.bulk.upload}",
            groupId = "${kafka.topics.nongovt.user.bulk.upload.group}")
    public void processNonGovtUserBulkUploadMessage(ConsumerRecord<String, String> data) {
        if (StringUtils.isBlank(data.value())) {
            logger.error("NonGovtUserBulkUploadConsumer:: processNonGovtUserBulkUploadMessage: Invalid Kafka Msg");
            return;
        }
        logger.info("NonGovtUserBulkUploadConsumer:: processNonGovtUserBulkUploadMessage: "
                + "Received event to initiate Non-Govt User Bulk Upload Process, orgId: {}", data.key());
        CompletableFuture.runAsync(() ->
                nonGovtUserBulkUploadProcessingService.initiateNonGovtUserBulkUploadProcess(data.value()));
    }
}
