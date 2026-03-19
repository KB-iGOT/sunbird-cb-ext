package org.sunbird.portal.badgedashboard.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.core.logger.CbExtLogger;
import org.sunbird.portal.badgedashboard.dto.BadgeDashboardDto;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class BadgeService {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final CbExtLogger logger = new CbExtLogger(getClass().getName());

    private final RedisCacheMgr redisCacheMgr;
    private final CbExtServerProperties serverProperties;

    public SBApiResponse getDashboardBadgeDetails() {
        SBApiResponse response = new SBApiResponse(Constants.BADGE_SUMMARY_API);
        response.setResponseCode(HttpStatus.OK);

        try {
            BadgeDashboardDto dto = new BadgeDashboardDto();

            dto.setTotalBadgeCount(getTrendDataFromHash(
                Constants.DASHBOARD_ALL_COURSE_BADGE_COUNT_DIFF, Constants.FIELD_TOTAL_BADGES));
            dto.setLiveCourseWithBadgeCount(getTrendDataFromHash(
                Constants.DASHBOARD_LIVE_COURSE_BADGE_COUNT_DIFF, Constants.FIELD_TOTAL_LIVE_BADGES));
            dto.setTotalBadgeAwardedCount(getTrendDataFromHash(
                Constants.DASHBOARD_TOTAL_BADGE_AWARDED_COUNT_DIFF, Constants.FIELD_BADGES_AWARDED));
            dto.setActiveLearners(getTrendDataFromHash(
                Constants.DASHBOARD_ACTIVE_LEARNERS_COUNT_DIFF, Constants.FIELD_ACTIVE_LEARNERS_DIFF));
            dto.setBadgeEarningRate(getTrendDataFromHash(
                Constants.DASHBOARD_BADGE_EARNING_RATE_DIFF, Constants.FIELD_BADGE_EARNED_LEARNERS));
            dto.setBadgePerformanceRate(getBadgePerformanceRates());
            dto.setContentCompletionRate(getContentCompletionRates());
            dto.setRecentBadgeActivity(getRecentBadgeActivityFromList());
            response.getParams().setStatus(Constants.STATUS);
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
     * Retrieves trend data from a Redis hash field
     */
    private BadgeDashboardDto.TrendData getTrendDataFromHash(String key, String field) {
        try {
            Map<String, String> hashData = redisCacheMgr.getAllHashFieldsFromDataRedis(key, serverProperties.getRedisBadgeDashboardIndex());
            if (hashData != null && hashData.containsKey(field)) {
                String jsonValue = hashData.get(field);
                if (jsonValue != null && !jsonValue.isEmpty()) {
                    Map<String, Object> data = OBJECT_MAPPER.readValue(jsonValue,
                        new TypeReference<Map<String, Object>>() {});

                    BadgeDashboardDto.TrendData trendData = new BadgeDashboardDto.TrendData();

                    Object totalCount = data.get(Constants.FIELD_TOTAL_COUNT);
                    if (totalCount != null) {
                        trendData.setTotalCount(((Number) totalCount).doubleValue());
                    }

                    Object countRate = data.get(Constants.FIELD_COUNT_RATE);
                    if (countRate != null) {
                        trendData.setCountRate(((Number) countRate).doubleValue());
                    }

                    Object trend = data.get(Constants.FIELD_TREND);
                    if (trend instanceof List) {
                        trendData.setTrend((List<String>) trend);
                    }

                    return trendData;
                }
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve trend data from hash key: " + key + ", field: " + field, e);
        }
        return null;
    }

    /**
     * Retrieves badge performance rates from Redis hash
     */
    private List<BadgeDashboardDto.BadgePerformanceRate> getBadgePerformanceRates() {
        List<BadgeDashboardDto.BadgePerformanceRate> result = new ArrayList<>();
        try {
            Map<String, String> hashData = redisCacheMgr.getAllHashFieldsFromDataRedis(
                Constants.DASHBOARD_BADGE_PERFORMANCE_RATE, serverProperties.getRedisBadgeDashboardIndex());

            if (hashData != null && !hashData.isEmpty()) {
                for (Map.Entry<String, String> entry : hashData.entrySet()) {
                    String badgeName = entry.getKey();
                    String jsonValue = entry.getValue();

                    if (jsonValue != null && !jsonValue.isEmpty()) {
                        Map<String, Object> data = OBJECT_MAPPER.readValue(jsonValue,
                            new TypeReference<Map<String, Object>>() {});

                        BadgeDashboardDto.BadgePerformanceRate rate =
                            new BadgeDashboardDto.BadgePerformanceRate();
                        rate.setBadgeName(badgeName);

                        Object rank = data.get(Constants.FIELD_RANK);
                        if (rank != null) {
                            rate.setRank(((Number) rank).intValue());
                        }

                        Object userCount = data.get(Constants.FIELD_USER_COUNT);
                        if (userCount != null) {
                            rate.setUserCount(((Number) userCount).intValue());
                        }

                        result.add(rate);
                    }
                }

                // Sort by rank
                result.sort((a, b) -> {
                    if (a.getRank() == null) return 1;
                    if (b.getRank() == null) return -1;
                    return a.getRank().compareTo(b.getRank());
                });
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve badge performance rates", e);
        }
        return result;
    }

    /**
     * Retrieves content completion rates from Redis hash
     */
    private List<BadgeDashboardDto.CourseCompletionRate> getContentCompletionRates() {
        List<BadgeDashboardDto.CourseCompletionRate> result = new ArrayList<>();
        try {
            Map<String, String> hashData = redisCacheMgr.getAllHashFieldsFromDataRedis(
                Constants.DASHBOARD_CONTENT_COMPLETION_RATE, serverProperties.getRedisBadgeDashboardIndex());

            if (hashData != null && !hashData.isEmpty()) {
                for (Map.Entry<String, String> entry : hashData.entrySet()) {
                    String courseName = entry.getKey();
                    String jsonValue = entry.getValue();

                    if (jsonValue != null && !jsonValue.isEmpty()) {
                        Map<String, Object> data = OBJECT_MAPPER.readValue(jsonValue,
                            new TypeReference<Map<String, Object>>() {});

                        BadgeDashboardDto.CourseCompletionRate rate =
                            new BadgeDashboardDto.CourseCompletionRate();
                        rate.setCourseName(courseName);

                        Object totalEnrolments = data.get(Constants.FIELD_TOTAL_ENROLMENTS);
                        if (totalEnrolments != null) {
                            rate.setTotalEnrolments(((Number) totalEnrolments).intValue());
                        }

                        Object totalCompletions = data.get(Constants.FIELD_TOTAL_COMPLETIONS_WITH_BADGE);
                        if (totalCompletions != null) {
                            rate.setTotalCompletionsWithBadge(((Number) totalCompletions).intValue());
                        }

                        result.add(rate);
                    }
                }

                // Sort by total enrollments descending
                result.sort((a, b) -> {
                    if (a.getTotalEnrolments() == null) return 1;
                    if (b.getTotalEnrolments() == null) return -1;
                    return b.getTotalEnrolments().compareTo(a.getTotalEnrolments());
                });
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve content completion rates", e);
        }
        return result;
    }

    /**
     * Retrieves recent badge activity from Redis list
     */
    private List<BadgeDashboardDto.RecentBadgeActivity> getRecentBadgeActivityFromList() {
        List<BadgeDashboardDto.RecentBadgeActivity> result = new ArrayList<>();
        try {
            // Fetch all items from the list (0 to -1 means all elements)
            List<String> listItems = redisCacheMgr.getListFromDataRedis(
                Constants.DASHBOARD_RECENT_BADGE_ACTIVITY, serverProperties.getRedisBadgeDashboardIndex(), 0, -1);

            if (listItems != null && !listItems.isEmpty()) {
                for (String jsonValue : listItems) {
                    if (jsonValue != null && !jsonValue.isEmpty()) {
                        Map<String, Object> data = OBJECT_MAPPER.readValue(jsonValue,
                            new TypeReference<Map<String, Object>>() {});

                        BadgeDashboardDto.RecentBadgeActivity activity =
                            new BadgeDashboardDto.RecentBadgeActivity();

                        Object userId = data.get(Constants.USER_ID);
                        if (userId != null) {
                            activity.setUserId(String.valueOf(userId));
                        }

                        Object userName = data.get(Constants.USER_NAME);
                        if (userName != null) {
                            activity.setUserName(String.valueOf(userName));
                        }

                        Object badgeId = data.get(Constants.BADGE_ID);
                        if (badgeId != null) {
                            activity.setBadgeId(String.valueOf(badgeId));
                        }

                        Object badgeTitle = data.get(Constants.BADGE_TITLE);
                        if (badgeTitle != null) {
                            activity.setBadgeTitle(String.valueOf(badgeTitle));
                        }

                        result.add(activity);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Failed to retrieve recent badge activity from list", e);
        }
        return result;
    }
}
