package org.sunbird.portal.badgedashboard.dto;

import java.util.List;

public class BadgeDashboardDto {

    private String liveCourseWithBadgeCount;
    private String totalBadgeAwardedCount;
    private List<BadgeAwardRate> badgeAwardRate;
    private List<BadgePerformanceRate> badgePerformanceRate;
    private List<CourseWithBadge> coursesWithBadges;
    private List<RecentBadgeActivity> recentBadgeActivity;

    public String getLiveCourseWithBadgeCount() {
        return liveCourseWithBadgeCount;
    }

    public void setLiveCourseWithBadgeCount(String liveCourseWithBadgeCount) {
        this.liveCourseWithBadgeCount = liveCourseWithBadgeCount;
    }

    public String getTotalBadgeAwardedCount() {
        return totalBadgeAwardedCount;
    }

    public void setTotalBadgeAwardedCount(String totalBadgeAwardedCount) {
        this.totalBadgeAwardedCount = totalBadgeAwardedCount;
    }

    public List<BadgeAwardRate> getBadgeAwardRate() {
        return badgeAwardRate;
    }

    public void setBadgeAwardRate(List<BadgeAwardRate> badgeAwardRate) {
        this.badgeAwardRate = badgeAwardRate;
    }

    public List<BadgePerformanceRate> getBadgePerformanceRate() {
        return badgePerformanceRate;
    }

    public void setBadgePerformanceRate(List<BadgePerformanceRate> badgePerformanceRate) {
        this.badgePerformanceRate = badgePerformanceRate;
    }

    public List<CourseWithBadge> getCoursesWithBadges() {
        return coursesWithBadges;
    }

    public void setCoursesWithBadges(List<CourseWithBadge> coursesWithBadges) {
        this.coursesWithBadges = coursesWithBadges;
    }

    public List<RecentBadgeActivity> getRecentBadgeActivity() {
        return recentBadgeActivity;
    }

    public void setRecentBadgeActivity(List<RecentBadgeActivity> recentBadgeActivity) {
        this.recentBadgeActivity = recentBadgeActivity;
    }

    // Inner classes for structured data
    public static class BadgeAwardRate {
        private String badge;
        private String awardRate;

        public BadgeAwardRate() {
        }

        public BadgeAwardRate(String badge, String awardRate) {
            this.badge = badge;
            this.awardRate = awardRate;
        }

        public String getBadge() {
            return badge;
        }

        public void setBadge(String badge) {
            this.badge = badge;
        }

        public String getAwardRate() {
            return awardRate;
        }

        public void setAwardRate(String awardRate) {
            this.awardRate = awardRate;
        }
    }

    public static class BadgePerformanceRate {
        private String badgeName;
        private String badgeCount;
        private String awardRate;

        public BadgePerformanceRate() {
        }

        public BadgePerformanceRate(String badgeName, String badgeCount, String awardRate) {
            this.badgeName = badgeName;
            this.badgeCount = badgeCount;
            this.awardRate = awardRate;
        }

        public String getBadgeName() {
            return badgeName;
        }

        public void setBadgeName(String badgeName) {
            this.badgeName = badgeName;
        }

        public String getBadgeCount() {
            return badgeCount;
        }

        public void setBadgeCount(String badgeCount) {
            this.badgeCount = badgeCount;
        }

        public String getAwardRate() {
            return awardRate;
        }

        public void setAwardRate(String awardRate) {
            this.awardRate = awardRate;
        }
    }

    public static class CourseWithBadge {
        private String courseName;
        private String badgesAwarded;
        private String badgeRate;

        public CourseWithBadge() {
        }

        public CourseWithBadge(String courseName, String badgesAwarded, String badgeRate) {
            this.courseName = courseName;
            this.badgesAwarded = badgesAwarded;
            this.badgeRate = badgeRate;
        }

        public String getCourseName() {
            return courseName;
        }

        public void setCourseName(String courseName) {
            this.courseName = courseName;
        }

        public String getBadgesAwarded() {
            return badgesAwarded;
        }

        public void setBadgesAwarded(String badgesAwarded) {
            this.badgesAwarded = badgesAwarded;
        }

        public String getBadgeRate() {
            return badgeRate;
        }

        public void setBadgeRate(String badgeRate) {
            this.badgeRate = badgeRate;
        }
    }

    public static class RecentBadgeActivity {
        private String userName;
        private String badgeTitle;

        public RecentBadgeActivity() {
        }

        public RecentBadgeActivity(String userName, String badgeTitle) {
            this.userName = userName;
            this.badgeTitle = badgeTitle;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getBadgeTitle() {
            return badgeTitle;
        }

        public void setBadgeTitle(String badgeTitle) {
            this.badgeTitle = badgeTitle;
        }
    }
}

