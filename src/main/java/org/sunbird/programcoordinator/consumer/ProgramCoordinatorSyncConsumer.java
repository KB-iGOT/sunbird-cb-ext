package org.sunbird.programcoordinator.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.common.util.Constants;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class ProgramCoordinatorSyncConsumer {

    private final ProgramCoordinatorSyncService syncService;
    private final ObjectMapper objectMapper;

    public ProgramCoordinatorSyncConsumer(ProgramCoordinatorSyncService syncService, ObjectMapper objectMapper) {
        this.syncService = syncService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${program.coordinator.sync.topic}",
            groupId = "${program.coordinator.sync.topic.group}")
    public void processProgramCoordinatorSyncMessage(
            ConsumerRecord<String, String> data) {

        log.info("Received Program Coordinator Sync Event");

        try {
            if (StringUtils.isBlank(data.value())) {
                log.error("Invalid Kafka message");
                return;
            }

            CompletableFuture.runAsync(() -> {
                try {
                    processSyncMessage(data.value());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });

        } catch (Exception e) {
            log.error("Error processing Program Coordinator Sync Event", e);
        }
    }

    @SuppressWarnings("unchecked")
    private void processSyncMessage(String message) throws Exception {

        Map<String, Object> event = objectMapper.readValue(message, Map.class);

        List<String> userIds = (List<String>) event.get(Constants.USER_IDS);

        if (CollectionUtils.isEmpty(userIds)) {
            log.error("Invalid coordinator sync event : {}", message);
            return;
        }

        syncService.syncUserProgramLookup(userIds);

        log.info("Synced lookup index for {} users", userIds.size());
    }


}
