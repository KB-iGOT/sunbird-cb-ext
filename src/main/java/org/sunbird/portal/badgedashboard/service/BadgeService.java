package org.sunbird.portal.badgedashboard.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.util.Constants;
import org.sunbird.core.logger.CbExtLogger;
import org.sunbird.portal.badgedashboard.dto.BadgeDashboardDto;
import org.sunbird.portal.badgedashboard.dto.BadgeDashboardDto.BadgeAwardRate;
import org.sunbird.portal.badgedashboard.dto.BadgeDashboardDto.BadgePerformanceRate;
import org.sunbird.portal.badgedashboard.dto.BadgeDashboardDto.CourseWithBadge;
import org.sunbird.portal.badgedashboard.dto.BadgeDashboardDto.RecentBadgeActivity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class BadgeService {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final CbExtLogger logger = new CbExtLogger(getClass().getName());

    private final RedisCacheMgr redisCacheMgr;

    public BadgeDashboardDto getDashboardBadgeDetails() {
        BadgeDashboardDto dto = new BadgeDashboardDto();

        dto.setLiveCourseWithBadgeCount(getStringFromRedis(Constants.DASHBOARD_LIVE_COURSE_BADGE_COUNT));
        dto.setTotalBadgeAwardedCount(getStringFromRedis(Constants.DASHBOARD_TOTAL_BADGE_AWARDED_COUNT));
        dto.setBadgeAwardRate(getBadgeAwardRateList(Constants.DASHBOARD_BADGE_AWARD_RATE));
        dto.setBadgePerformanceRate(getBadgePerformanceRateList(Constants.DASHBOARD_BADGE_PERFORMANCE_RATE));
        dto.setCoursesWithBadges(getCoursesWithBadgesList(Constants.DASHBOARD_COURSES_WITH_BADGES));
        dto.setRecentBadgeActivity(getRecentBadgeActivityList(Constants.DASHBOARD_RECENT_BADGE_ACTIVITY));
        return dto;
    }

    private String getStringFromRedis(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve value from Redis for key: " + key, e);
        }
        return null;
    }

    /**
     * Retrieves badge award rate list from Redis.
     * Note: null argument to getCache(key, null) means use default Redis database (index 0)
     */
    private List<BadgeAwardRate> getBadgeAwardRateList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null && !value.isEmpty()) {
                List<Map<String, Object>> rawList = OBJECT_MAPPER.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<BadgeAwardRate> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    BadgeAwardRate rate = new BadgeAwardRate();
                    rate.setBadge(getStringValue(item.get("badge")));
                    rate.setAwardRate(getStringValue(item.get("award_rate")));
                    result.add(rate);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("Failed to parse badge award rate list from Redis key: " + key, e);
        }
        return Collections.emptyList();
    }

    /**
     * Retrieves badge performance rate list from Redis.
     * Note: null argument to getCache(key, null) means use default Redis database (index 0)
     */
    private List<BadgePerformanceRate> getBadgePerformanceRateList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null && !value.isEmpty()) {
                List<Map<String, Object>> rawList = OBJECT_MAPPER.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<BadgePerformanceRate> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    BadgePerformanceRate rate = new BadgePerformanceRate();
                    rate.setBadgeName(getStringValue(item.get("badge_name")));
                    rate.setBadgeCount(getStringValue(item.get("badge_count")));
                    rate.setAwardRate(getStringValue(item.get("award_rate")));
                    result.add(rate);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("Failed to parse badge performance rate list from Redis key: " + key, e);
        }
        return Collections.emptyList();
    }

    /**
     * Retrieves courses with badges list from Redis.
     * Note: null argument to getCache(key, null) means use default Redis database (index 0)
     */
    private List<CourseWithBadge> getCoursesWithBadgesList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null && !value.isEmpty()) {
                List<Map<String, Object>> rawList = OBJECT_MAPPER.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<CourseWithBadge> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    CourseWithBadge course = new CourseWithBadge();
                    course.setCourseName(getStringValue(item.get("course_name")));
                    course.setBadgesAwarded(getStringValue(item.get("badges_awarded")));
                    course.setBadgeRate(getStringValue(item.get("badge_rate")));
                    result.add(course);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("Failed to parse courses with badges list from Redis key: " + key, e);
        }
        return Collections.emptyList();
    }

    /**
     * Retrieves recent badge activity list from Redis.
     * Note: null argument to getCache(key, null) means use default Redis database (index 0)
     */
    private List<RecentBadgeActivity> getRecentBadgeActivityList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null && !value.isEmpty()) {
                List<Map<String, Object>> rawList = OBJECT_MAPPER.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<RecentBadgeActivity> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    RecentBadgeActivity activity = new RecentBadgeActivity();
                    activity.setUserName(getStringValue(item.get("userName")));
                    activity.setBadgeTitle(getStringValue(item.get("badge")));
                    result.add(activity);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error("Failed to parse recent badge activity list from Redis key: " + key, e);
        }
        return Collections.emptyList();
    }

    private String getStringValue(Object value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }
}
