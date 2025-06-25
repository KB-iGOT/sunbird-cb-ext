package org.sunbird.user.util.notificationUtill;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.sunbird.common.util.Constants.*;

@Service
@Slf4j
public class NotificationTriggerService {


    @Value("${notification.api.url}")
    private String notificationApiUrl;

    static final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    private ObjectMapper objectMapper;

    public void sendNotification(
            String subCategory,
            String subType,
            List<String> userIds,
            Map<String, Object> message
    ) {
        try {
            if (!StringUtils.hasText(subCategory)) {
                throw new IllegalArgumentException("subCategory is required");
            }
            if (!StringUtils.hasText(subType)) {
                throw new IllegalArgumentException("subType is required");
            }

            if (CollectionUtils.isEmpty(userIds)) {
                throw new IllegalArgumentException("userIds cannot be null or empty");
            }

            if (message == null || message.isEmpty()) {
                throw new IllegalArgumentException("message cannot be null or empty");
            }

            Map<String, Object> payload = new HashMap<>();
            payload.put(SUB_CATEGORY, subCategory);
            payload.put(SUB_TYPE,subType);
            payload.put(USER_IDS, userIds);
            payload.put(MESSAGE, message);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);

            restTemplate.postForEntity(notificationApiUrl, request, Map.class);

        } catch (IllegalArgumentException iae) {
            log.warn("Invalid input for sendNotification: {}", iae.getMessage());

        } catch (HttpClientErrorException hce) {
            log.error("HTTP error while sending notification: {}", hce.getResponseBodyAsString(), hce);

        } catch (Exception e) {
            log.error("Unexpected error while sending notification: {}", e.getMessage(), e);
        }
    }

    public void triggerNotification(
            String subCategory,
            String subType,
            List<String> userIds,
            String userName,
            String courseName,
            Map<String, Object> data
    ) {
        ObjectNode placeholders = objectMapper.createObjectNode();
        placeholders.put(USER_NAME, userName);
        placeholders.put(TITLE,courseName);
        Map<String, Object> message = new HashMap<>();
        message.put(PLACE_HOLDERS, placeholders);
        message.put(DATA, data);
        log.info("notifications message in triggerNotification:{}", message);

        try {
            sendNotification(subCategory,subType, userIds, message);
            log.info("Notification sent successfully for subCategory: {}", subCategory);
        } catch (Exception e) {
            log.error("Notification failed for subCategory: {}", subCategory, e);
        }
    }
}