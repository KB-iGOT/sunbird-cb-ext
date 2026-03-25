package org.sunbird.walloffame.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.sunbird.common.util.Constants;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = Constants.NLW_USER_LEADERBOARD)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class UserLeaderboardEntity {
    @Id
    @Column(name = "userid")
    private String userId;

    @Column(name = "row_num")
    private Integer rowNum;

    @Column(name = "count")
    private Integer count;

    @Column(name = "designation")
    private String designation;

    @Column(name = "fullname")
    private String fullName;

    @Column(name = "last_credit_date")
    private String lastCreditDate;

    @Column(name = "org_id")
    private String orgId;

    @Column(name = "profile_image")
    private String profileImage;

    @Column(name = "rank")
    private Integer rank;

    @Column(name = "total_learning_hours")
    private String totalLearningHours;

    @Column(name = "total_points")
    private Long totalPoints;

    @Column(name = "total_badges")
    private Long totalBadges;

}
