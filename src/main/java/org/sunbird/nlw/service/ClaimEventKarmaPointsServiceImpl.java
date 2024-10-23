package org.sunbird.nlw.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.core.producer.Producer;

import java.util.HashMap;
import java.util.Map;

@Service
public class ClaimEventKarmaPointsServiceImpl {


    @Autowired
    Producer producer;

    @Autowired
    CbExtServerProperties serverProperties;

    public void generateKarmaPointEventAndPushToKafka(String userId, String eventId, String batchId) {

        long ets = System.currentTimeMillis();

        Map<String, Object> objectMap = new HashMap<>();
        objectMap.put("user_id", userId);
        objectMap.put("etc", ets);
        objectMap.put("event_id", eventId);
        objectMap.put("batch_id", batchId);

        producer.pushWithKey(serverProperties.getUserEventKarmaPointTopic(), objectMap, userId);

    }
}