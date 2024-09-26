package org.sunbird.consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.sunbird.cqfassessment.service.CQFAssessmentService;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @author mahesh.vakkund
 */

@Service
public class CQFConsumer {
    Logger logger = LogManager.getLogger(CQFConsumer.class);

    @Autowired
    private ObjectMapper mapper;


    @Autowired
    private CQFAssessmentService cqfAssessmentService;


    @KafkaListener(groupId = "${kafka.topics.user.assessment.async.submit.handler.group}", topics = "${kafka.topics.cqf.assessment.postpublish}")
    public void processMessage(ConsumerRecord<String, String> data) {
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> processCQFPostPublish(data.value()));
            } else {
                logger.error("Error in CQFConsumer : Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in CQFConsumer: Error Msg :%s", e.getMessage()), e);
        }
    }


    /**
     * Process a CQF post-publish event by delegating to the CQFAssessmentService.
     * <p>
     * This method is a pass-through to the CQFAssessmentService and does not perform any additional logic.
     *
     * @param assessmentId the ID of the assessment
     */
    private void processCQFPostPublish(String assessmentId) {
        try {
            String assessmentIdStr = assessmentId.replace("\"", "");
            cqfAssessmentService.processCQFPostPublish(assessmentIdStr);
        } catch (Exception e) {
            logger.error("Error delegating CQF post-publish event to CQFAssessmentService for assessment ID: {}", assessmentId, e);
        }

    }
}
