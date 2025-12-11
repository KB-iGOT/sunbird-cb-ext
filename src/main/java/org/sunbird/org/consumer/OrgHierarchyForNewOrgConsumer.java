package org.sunbird.org.consumer;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
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
        String orgId = input.get(Constants.ORG_ID);
        if (StringUtils.isBlank(sbRootOrgId) || StringUtils.isBlank(orgName) || StringUtils.isBlank(orgId)) {
            logger.error("Invalid input for term creation: required fields missing. Payload={}", input);
            return;
        }
        String frameworkId = sbRootOrgId + Constants.ORG_HIERARCHY_SUFFIX;
        String category = getCategoryForTermCreation(frameworkId, 0);
        if (StringUtils.isBlank(category)) {
            logger.error("Category not available or framework read failed for frameworkId={}", frameworkId);
            return;
        }

        // Construct term object
        Map<String, Object> term = new HashMap<>();
        term.put(Constants.NAME, orgName);
        term.put(Constants.DESCRIPTION, input.get(Constants.DESCRIPTION));
        term.put(Constants.CODE, UUIDs.timeBased().toString());

        Map<String, Object> additionalProps = new HashMap<>();
        additionalProps.put(Constants.ORG_ID, orgId);
        term.put(Constants.ADDITIONAL_PROPERTIES, additionalProps);

        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.TERM, term);
        requestBody.put(Constants.REQUEST, request);

        // Construct URL
        String createUrl = serverProperties.getKmBaseHost()
                + serverProperties.getKmFrameworkTermCreatePath()
                + "?framework=" + frameworkId
                + "&category=" + category;

        logger.info("Creating framework term: orgId={}, framework={}", orgId, frameworkId);
        Map<String, Object> response = outboundRequestHandler.fetchResultUsingPost(createUrl, requestBody, null);
        if (response != null && response.get(Constants.RESPONSE_CODE) != null && "OK".equalsIgnoreCase(String.valueOf(response.get(Constants.RESPONSE_CODE)))) {
            logger.info("Term created successfully for framework {}", frameworkId);
            publishFramework(frameworkId, sbRootOrgId);
        } else {
            logger.error("Failed to create term for framework {}. Response={}", frameworkId, response);
        }
    }

    private void publishFramework(String frameworkId, String orgId) {
        logger.info("Publishing framework with created latest term");
        String publishUrl = serverProperties.getKmBaseHost()
                + serverProperties.getKmFrameworkPublishPath()
                + "/" + frameworkId;

        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.CONTENT_TYPE, "application/json");
        headers.put(Constants.X_CHANNEL_ID, orgId);
        logger.info("Publishing framework {}", frameworkId);
        Map<String, Object> response = outboundRequestHandler.fetchResultUsingPost(publishUrl, new HashMap<>(), headers);
        if (MapUtils.isNotEmpty(response) && "OK".equalsIgnoreCase((String) response.get("responseCode"))) {
            logger.info("Framework published successfully.");
        } else {
            logger.error("Failed to publish framework: {}", response);
        }
    }

    private String getCategoryForTermCreation(String frameworkId, int index) {

        String url = serverProperties.getKmBaseHost()
                + serverProperties.getFrameworkReadEndpoint()
                + "/" + frameworkId;

        logger.info("Reading framework from: {}", url);
        Map<String, Object> response;
        try {
            response = (Map<String, Object>) outboundRequestHandler.fetchResult(url);
        } catch (Exception e) {
            logger.error("Error fetching framework {}: {}", frameworkId, e.getMessage());
            return null;
        }
        if (MapUtils.isEmpty(response) || MapUtils.isEmpty((Map) response.get(Constants.RESULT))) {
            logger.error("Invalid framework response: {}", response);
            return null;
        }
        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
        Map<String, Object> framework = (Map<String, Object>) result.get(Constants.FRAMEWORK);
        if (MapUtils.isEmpty(framework)) {
            logger.error("Framework object missing for {}", frameworkId);
            return null;
        }
        List<Map<String, Object>> categories = (List<Map<String, Object>>) framework.get(Constants.CATEGORIES);
        if (CollectionUtils.isEmpty(categories) || index >= categories.size()) {
            logger.error("No category found at index {} for framework {}", index, frameworkId);
            return null;
        }
        return String.valueOf(categories.get(index).get(Constants.NAME));
    }
}
