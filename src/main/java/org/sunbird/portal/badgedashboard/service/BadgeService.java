package org.sunbird.portal.badgedashboard.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.util.Constants;
import org.sunbird.core.logger.CbExtLogger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class BadgeService {

    private final CbExtLogger logger = new CbExtLogger(getClass().getName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private RedisCacheMgr redisCacheMgr;

    public Map<String, Object> getDashboardBadgeDetails() {
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.DASHBOARD_LIVE_COURSE_BADGE_COUNT,
                getScalarValue(Constants.DASHBOARD_LIVE_COURSE_BADGE_COUNT, ""));

        response.put(Constants.DASHBOARD_TOTAL_BADGE_AWARDED_COUNT,
                getScalarValue(Constants.DASHBOARD_TOTAL_BADGE_AWARDED_COUNT, ""));

        response.put(Constants.DASHBOARD_BADGE_AWARD_RATE,
                getListValue(Constants.DASHBOARD_BADGE_AWARD_RATE));

        response.put(Constants.DASHBOARD_BADGE_PERFORMANCE_RATE,
                getListValue(Constants.DASHBOARD_BADGE_PERFORMANCE_RATE));

        response.put(Constants.DASHBOARD_COURSES_WITH_BADGES,
                getListValue(Constants.DASHBOARD_COURSES_WITH_BADGES));

        response.put(Constants.DASHBOARD_RECENT_BADGE_ACTIVITY,
                getListValue(Constants.DASHBOARD_RECENT_BADGE_ACTIVITY));

        return response;
    }

    private String getScalarValue(String key, String defaultValue) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            return (value != null) ? value : defaultValue;
        } catch (Exception e) {
            logger.error(e);
            return defaultValue;
        }
    }

    private List<Map<String, Object>> getListValue(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null) {
                return objectMapper.readValue(value, new TypeReference<List<Map<String, Object>>>() {});
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return Collections.emptyList();
    }
}
