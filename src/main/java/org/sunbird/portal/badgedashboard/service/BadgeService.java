package org.sunbird.portal.badgedashboard.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.model.SBApiResponse;
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

    public SBApiResponse getDashboardBadgeDetails() {
        SBApiResponse response = new SBApiResponse(Constants.BADGE_SUMMARY_API);
        response.setResponseCode(HttpStatus.OK);

        try {
            BadgeDashboardDto dto = new BadgeDashboardDto();

            dto.setLiveCourseWithBadgeCount(getStringFromRedis(Constants.DASHBOARD_LIVE_COURSE_BADGE_COUNT));
            dto.setTotalBadgeAwardedCount(getStringFromRedis(Constants.DASHBOARD_TOTAL_BADGE_AWARDED_COUNT));
            dto.setBadgeAwardRate(getBadgeAwardRateList(Constants.DASHBOARD_BADGE_AWARD_RATE));
            dto.setBadgePerformanceRate(getBadgePerformanceRateList(Constants.DASHBOARD_BADGE_PERFORMANCE_RATE));
            dto.setCoursesWithBadges(getCoursesWithBadgesList(Constants.DASHBOARD_COURSES_WITH_BADGES));
            dto.setRecentBadgeActivity(getRecentBadgeActivityList(Constants.DASHBOARD_RECENT_BADGE_ACTIVITY));

            response.put(Constants.BADGE_DETAILS, dto);
        } catch (Exception e) {
            logger.error("Failed to fetch dashboard badge details", e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrmsg(Constants.BADGE_DETAILS_FETCH_ERROR);
            response.getParams().setStatus(Constants.FAILED);
        }
        return response;
    }

    /**
     * Retrieves a string value from Redis.
     * @param key Redis key to retrieve
     * @return String value or empty string if not found
     * Note: null argument to getCache(key, null) means use default Redis database (index 0)
     */
    private String getStringFromRedis(String key) {
        try {
            String value = redisCacheMgr.getCache(key, null);
            if (value != null && !value.isEmpty()) {
                return value;
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve value from Redis for key: " + key, e);
        }
        return "";
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
                    rate.setBadge(getStringValue(item.get(Constants.BADGE)));
                    rate.setAwardRate(getStringValue(item.get(Constants.AWARD_RATE)));
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
                    rate.setBadgeName(getStringValue(item.get(Constants.BADGE_NAME)));
                    rate.setBadgeCount(getStringValue(item.get(Constants.BADGE_COUNT)));
                    rate.setAwardRate(getStringValue(item.get(Constants.AWARD_RATE)));
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
                    course.setCourseName(getStringValue(item.get(Constants.COURSE_NAME_KEY)));
                    course.setBadgesAwarded(getStringValue(item.get(Constants.BADGES_AWARDED)));
                    course.setBadgeRate(getStringValue(item.get(Constants.BADGE_RATE)));
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
                    activity.setUserName(getStringValue(item.get(Constants.USER_NAME_KEY)));
                    activity.setBadgeTitle(getStringValue(item.get(Constants.BADGE)));
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
