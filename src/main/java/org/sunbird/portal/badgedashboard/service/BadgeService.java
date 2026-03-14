package org.sunbird.portal.badgedashboard.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
public class BadgeService {

    private final CbExtLogger logger = new CbExtLogger(getClass().getName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private RedisCacheMgr redisCacheMgr;

    public BadgeDashboardDto getDashboardBadgeDetails() {
        BadgeDashboardDto dto = new BadgeDashboardDto();

        // Set scalar values
        dto.setLiveCourseWithBadgeCount(getScalarValue(Constants.DASHBOARD_LIVE_COURSE_BADGE_COUNT, ""));
        dto.setTotalBadgeAwardedCount(getScalarValue(Constants.DASHBOARD_TOTAL_BADGE_AWARDED_COUNT, ""));

        // Set badge award rate list
        dto.setBadgeAwardRate(getBadgeAwardRateList(Constants.DASHBOARD_BADGE_AWARD_RATE));

        // Set badge performance rate list
        dto.setBadgePerformanceRate(getBadgePerformanceRateList(Constants.DASHBOARD_BADGE_PERFORMANCE_RATE));

        // Set courses with badges list
        dto.setCoursesWithBadges(getCoursesWithBadgesList(Constants.DASHBOARD_COURSES_WITH_BADGES));

        // Set recent badge activity list
        dto.setRecentBadgeActivity(getRecentBadgeActivityList(Constants.DASHBOARD_RECENT_BADGE_ACTIVITY));

        return dto;
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

    private List<BadgeAwardRate> getBadgeAwardRateList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null) {
                List<Map<String, Object>> rawList = objectMapper.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<BadgeAwardRate> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    BadgeAwardRate rate = new BadgeAwardRate();
                    rate.setBadge((String) item.get("badge"));
                    rate.setAwardRate(getDoubleValue(item.get("award_rate")));
                    result.add(rate);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return Collections.emptyList();
    }

    private List<BadgePerformanceRate> getBadgePerformanceRateList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null) {
                List<Map<String, Object>> rawList = objectMapper.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<BadgePerformanceRate> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    BadgePerformanceRate rate = new BadgePerformanceRate();
                    rate.setBadgeName((String) item.get("badge_name"));
                    rate.setBadgeCount(String.valueOf(item.get("badge_count")));
                    rate.setAwardRate(String.valueOf(item.get("award_rate")));
                    result.add(rate);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return Collections.emptyList();
    }

    private List<CourseWithBadge> getCoursesWithBadgesList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null) {
                List<Map<String, Object>> rawList = objectMapper.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<CourseWithBadge> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    CourseWithBadge course = new CourseWithBadge();
                    course.setCourseName((String) item.get("course_name"));
                    course.setBadgesAwarded(String.valueOf(item.get("badges_awarded")));
                    course.setBadgeRate(String.valueOf(item.get("badge_rate")));
                    result.add(course);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return Collections.emptyList();
    }

    private List<RecentBadgeActivity> getRecentBadgeActivityList(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null) {
                List<Map<String, Object>> rawList = objectMapper.readValue(value,
                    new TypeReference<List<Map<String, Object>>>() {});

                List<RecentBadgeActivity> result = new ArrayList<>();
                for (Map<String, Object> item : rawList) {
                    RecentBadgeActivity activity = new RecentBadgeActivity();
                    activity.setUserName((String) item.get("userName"));
                    activity.setBadge((String) item.get("badge"));
                    result.add(activity);
                }
                return result;
            }
        } catch (Exception e) {
            logger.error(e);
        }
        return Collections.emptyList();
    }

    private Double getDoubleValue(Object value) {
        if (value == null) {
            return 0.0;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            logger.error("Failed to parse double value: " + value, e);
            return 0.0;
        }
    }
}
