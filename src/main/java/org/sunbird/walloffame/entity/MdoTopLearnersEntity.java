package org.sunbird.walloffame.entity;

import lombok.*;
import org.sunbird.common.util.Constants;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = Constants.TABLE_TOP_10_LEARNER)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@IdClass(MdoTopLearnersEntity.MdoTopLearnersKey.class)
public class MdoTopLearnersEntity {

    @Id
    @Column(name = "org_id")
    private String orgId;

    @Id
    @Column(name = "row_num")
    private Integer rowNum;

    @Column(name = "designation")
    private String designation;

    @Column(name = "fullname")
    private String fullname;

    @Column(name = "month")
    private String month;

    @Column(name = "org_name")
    private String orgName;

    @Column(name = "previous_rank")
    private Integer previousRank;

    @Column(name = "profile_image")
    private String profileImage;

    @Column(name = "rank")
    private Integer rank;

    @Column(name = "total_points")
    private Long totalPoints;

    @Column(name = "userid")
    private String userId;

    @Column(name = "year")
    private String year;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MdoTopLearnersKey implements Serializable {
        private String orgId;
        private Integer rowNum;
    }
}
