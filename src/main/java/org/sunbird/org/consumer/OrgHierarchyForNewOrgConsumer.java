package org.sunbird.org.consumer;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Component
public class OrgHierarchyForNewOrgConsumer {

    private static final Logger logger = LoggerFactory.getLogger(OrgHierarchyForNewOrgConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;


    @KafkaListener(topics = "${kafka.topics.org.hierarchy.framework.new.org.event}", groupId = "${kafka.topics.org.hierarchy.framework.new.org.event.group}")
    public void processOrgHierarchyCreationForNewOrg(ConsumerRecord<String, String> data) {

        logger.info("Received event for new org hierarchy creation.");
        logger.debug("Kafka Message: {}", data.value());

        if (StringUtils.isBlank(data.value())) {
            logger.error("Empty Kafka message received.");
            return;
        }

        CompletableFuture.runAsync(() -> {
            try {
                initiateOrgHierarchyCreationForNewOrg(data.value());
            } catch (Exception e) {
                logger.error("Async error while processing org hierarchy", e);
                logger.error("Payload causing failure: {}", data.value());
            }
        });
    }


    private void initiateOrgHierarchyCreationForNewOrg(String value) {
        long startTime = System.currentTimeMillis();
        logger.info("OrgHierarchy creation started.");

        try {
            HashMap<String, String> inputData = objectMapper.readValue(
                    value,
                    new TypeReference<HashMap<String, String>>() {
                    }
            );
            List<String> errors = validateReceivedKafkaMessage(inputData);
            if (!errors.isEmpty()) {
                logger.error("Kafka message validation failed: {}", errors);
                return;
            }
            createTermAndPublishFrameworkHierarchy(inputData);
        } catch (Exception e) {
            logger.error("Error processing orgHierarchy creation", e);
        }

        logger.info("OrgHierarchy creation completed in {} ms",
                (System.currentTimeMillis() - startTime));
    }


    private List<String> validateReceivedKafkaMessage(Map<String, String> input) {
        List<String> errors = new ArrayList<>();

        if (StringUtils.isBlank(input.get(Constants.SB_ROOT_ORG_ID))) {
            errors.add("sbRootOrgId missing");
        }
        if (StringUtils.isEmpty(input.get(Constants.ORG_NAME))) {
            errors.add("orgName missing");
        }

        if (StringUtils.isEmpty(input.get(Constants.ORG_ID))) {
            errors.add("orgId missing");
        }
        return errors;
    }

    public void createTermAndPublishFrameworkHierarchy(Map<String, String> input) {

        String sbRootOrgId = input.get(Constants.SB_ROOT_ORG_ID);
        String orgName = input.get(Constants.ORG_NAME);
        String l0frameworkId = sbRootOrgId + Constants.ORG_HIERARCHY_SUFFIX;

        String url = serverProperties.getKmBaseHost() + serverProperties.getFrameworkReadEndpoint() + Constants.SLASH + l0frameworkId;
        logger.info("OrgHierarchyForNewOrgConsumer::createTermAndPublishFrameworkHierarchy:framework read url:: " + url);
        Map<String, Object> frameworkReadResponse = (Map<String, Object>) outboundRequestHandler.fetchResult(url);
        // Check if the framework read response is null or does not contain the result key
        if (frameworkReadResponse == null || !frameworkReadResponse.containsKey(Constants.RESULT)) {
            logger.info("OrgHierarchyForNewOrgConsumer::createTermAndPublishFrameworkHierarchy:Failed to read framework");
            return;
        }

        // Construct term object
        Map<String, Object> term = new HashMap<>();
        term.put(Constants.NAME, orgName);
        term.put(Constants.DESCRIPTION, "Auto-created level-one term for new org");
        term.put(Constants.CODE, UUIDs.timeBased().toString());

        Map<String, Object> additionalProps = new HashMap<>();
        additionalProps.put(Constants.ORG_ID, input.get(Constants.ORG_ID));
        term.put(Constants.ADDITIONAL_PROPERTIES, additionalProps);

        Map<String, Object> request = new HashMap<>();
        Map<String, Object> reqWrapper = new HashMap<>();
        reqWrapper.put(Constants.TERM, term);
        request.put(Constants.REQUEST, reqWrapper);

        // Construct URL
        String createUrl = serverProperties.getKmBaseHost()
                + serverProperties.getKmFrameworkTermCreatePath()
                + "?framework=" + l0frameworkId
                + "&category=LevelOne";

        logger.info("Creating framework term for orgId={}", sbRootOrgId);

        Map<String, Object> response = outboundRequestHandler.fetchResultUsingPost(
                createUrl, request, null
        );

        if (MapUtils.isNotEmpty(response)
                && "OK".equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {

            logger.info("Term created successfully for framework {}", l0frameworkId);
            publishFramework(l0frameworkId, sbRootOrgId);

        } else {
            logger.error("Failed to create term. Response: {}", response);
        }
    }


    private void publishFramework(String frameworkId, String orgId) {

        String publishUrl = serverProperties.getKmBaseHost()
                + serverProperties.getKmFrameworkPublishPath()
                + "/" + frameworkId;

        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.CONTENT_TYPE, "application/json");
        headers.put(Constants.X_CHANNEL_ID, orgId);

        logger.info("Publishing framework {}", frameworkId);

        Map<String, Object> response =
                outboundRequestHandler.fetchResultUsingPost(publishUrl, new HashMap<>(), headers);

        if (MapUtils.isNotEmpty(response)
                && "OK".equalsIgnoreCase((String) response.get("responseCode"))) {
            logger.info("Framework published successfully.");
        } else {
            logger.error("Failed to publish framework: {}", response);
        }
    }
}
