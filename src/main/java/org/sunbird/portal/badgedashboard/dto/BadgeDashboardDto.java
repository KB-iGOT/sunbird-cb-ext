package org.sunbird.portal.badgedashboard.dto;

import java.util.List;

public class BadgeDashboardDto {


    private TrendData totalBadgeCount;
    private TrendData liveCourseWithBadgeCount;
    private TrendData totalBadgeAwardedCount;
    private TrendData activeLearners;
    private TrendData badgeEarningRate;
    private List<BadgePerformanceRate> badgePerformanceRate;
    private List<CourseCompletionRate> contentCompletionRate;
    private List<RecentBadgeActivity> recentBadgeActivity;


    public List<BadgePerformanceRate> getBadgePerformanceRate() {
        return badgePerformanceRate;
    }

    public void setBadgePerformanceRate(List<BadgePerformanceRate> badgePerformanceRate) {
        this.badgePerformanceRate = badgePerformanceRate;
    }

    public List<CourseCompletionRate> getContentCompletionRate() {
        return contentCompletionRate;
    }

    public void setContentCompletionRate(List<CourseCompletionRate> contentCompletionRate) {
        this.contentCompletionRate = contentCompletionRate;
    }

    public List<RecentBadgeActivity> getRecentBadgeActivity() {
        return recentBadgeActivity;
    }

    public void setRecentBadgeActivity(List<RecentBadgeActivity> recentBadgeActivity) {
        this.recentBadgeActivity = recentBadgeActivity;
    }

    public TrendData getLiveCourseWithBadgeCount() {
        return liveCourseWithBadgeCount;
    }

    public void setLiveCourseWithBadgeCount(TrendData liveCourseWithBadgeCount) {
        this.liveCourseWithBadgeCount = liveCourseWithBadgeCount;
    }

    public TrendData getTotalBadgeCount() {
        return totalBadgeCount;
    }

    public void setTotalBadgeCount(TrendData totalBadgeCount) {
        this.totalBadgeCount = totalBadgeCount;
    }

    public TrendData getTotalBadgeAwardedCount() {
        return totalBadgeAwardedCount;
    }

    public void setTotalBadgeAwardedCount(TrendData totalBadgeAwardedCount) {
        this.totalBadgeAwardedCount = totalBadgeAwardedCount;
    }

    public TrendData getActiveLearners() {
        return activeLearners;
    }

    public void setActiveLearners(TrendData activeLearners) {
        this.activeLearners = activeLearners;
    }

    public TrendData getBadgeEarningRate() {
        return badgeEarningRate;
    }

    public void setBadgeEarningRate(TrendData badgeEarningRate) {
        this.badgeEarningRate = badgeEarningRate;
    }

    /**
     * Trend data with count, rate and trend direction
     */
    public static class TrendData {
        private Double totalCount;
        private Double countRate;
        private List<String> trend;

        public TrendData() {
        }

        public TrendData(Double totalCount, Double countRate, List<String> trend) {
            this.totalCount = totalCount;
            this.countRate = countRate;
            this.trend = trend;
        }

        public Double getTotalCount() {
            return totalCount;
        }

        public void setTotalCount(Double totalCount) {
            this.totalCount = totalCount;
        }

        public Double getCountRate() {
            return countRate;
        }

        public void setCountRate(Double countRate) {
            this.countRate = countRate;
        }

        public List<String> getTrend() {
            return trend;
        }

        public void setTrend(List<String> trend) {
            this.trend = trend;
        }
    }

    /**
     * Badge performance data with rank and user count
     */
    public static class BadgePerformanceRate {
        private String badgeName;
        private Integer rank;
        private Integer userCount;

        public BadgePerformanceRate() {
        }

        public BadgePerformanceRate(String badgeName, Integer rank, Integer userCount) {
            this.badgeName = badgeName;
            this.rank = rank;
            this.userCount = userCount;
        }

        public String getBadgeName() {
            return badgeName;
        }

        public void setBadgeName(String badgeName) {
            this.badgeName = badgeName;
        }

        public Integer getRank() {
            return rank;
        }

        public void setRank(Integer rank) {
            this.rank = rank;
        }

        public Integer getUserCount() {
            return userCount;
        }

        public void setUserCount(Integer userCount) {
            this.userCount = userCount;
        }
    }

    /**
     * Course completion rate with enrollments and completions
     */
    public static class CourseCompletionRate {
        private String courseName;
        private Integer totalEnrolments;
        private Integer totalCompletionsWithBadge;

        public CourseCompletionRate() {
        }

        public CourseCompletionRate(String courseName, Integer totalEnrolments, Integer totalCompletionsWithBadge) {
            this.courseName = courseName;
            this.totalEnrolments = totalEnrolments;
            this.totalCompletionsWithBadge = totalCompletionsWithBadge;
        }

        public String getCourseName() {
            return courseName;
        }

        public void setCourseName(String courseName) {
            this.courseName = courseName;
        }

        public Integer getTotalEnrolments() {
            return totalEnrolments;
        }

        public void setTotalEnrolments(Integer totalEnrolments) {
            this.totalEnrolments = totalEnrolments;
        }

        public Integer getTotalCompletionsWithBadge() {
            return totalCompletionsWithBadge;
        }

        public void setTotalCompletionsWithBadge(Integer totalCompletionsWithBadge) {
            this.totalCompletionsWithBadge = totalCompletionsWithBadge;
        }
    }

    public static class RecentBadgeActivity {
        private String userId;
        private String userName;
        private String badgeId;
        private String badgeTitle;

        public RecentBadgeActivity() {
        }

        public RecentBadgeActivity(String userId, String userName, String badgeId, String badgeTitle) {
            this.userId = userId;
            this.userName = userName;
            this.badgeId = badgeId;
            this.badgeTitle = badgeTitle;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getBadgeId() {
            return badgeId;
        }

        public void setBadgeId(String badgeId) {
            this.badgeId = badgeId;
        }

        public String getBadgeTitle() {
            return badgeTitle;
        }

        public void setBadgeTitle(String badgeTitle) {
            this.badgeTitle = badgeTitle;
        }
    }
}

