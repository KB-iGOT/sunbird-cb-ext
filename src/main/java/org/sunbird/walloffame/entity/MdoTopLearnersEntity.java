package org.sunbird.walloffame.entity;

import lombok.*;
import org.springframework.data.cassandra.core.mapping.Column;
import org.sunbird.common.util.Constants;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import java.io.Serializable;
import java.time.Instant;

@Entity
@Table(name = Constants.TABLE_TOP_10_LEARNER)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@IdClass(MdoTopLearnersEntity.MdoTopLearnersKey.class)
public class MdoTopLearnersEntity {

    @Id
    @Column("org_id")
    private String orgId;

    @Id
    @Column("row_num")
    private Integer rowNum;

    @Column("designation")
    private String designation;

    @Column("fullname")
    private String fullname;

    @Column("last_credit_date")
    private Instant lastCreditDate;

    @Column("month")
    private String month;

    @Column("org_name")
    private String orgName;

    @Column("previous_rank")
    private Integer previousRank;

    @Column("profile_image")
    private String profileImage;

    @Column("rank")
    private Integer rank;

    @Column("total_points")
    private Long totalPoints;

    @Column("userid")
    private String userId;

    @Column("year")
    private String year;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MdoTopLearnersKey implements Serializable {
        private String orgId;
        private Integer rowNum;
    }
}
