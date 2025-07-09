package org.sunbird.insights.entity;

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
@Table(name = Constants.LEARNER_STATS)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class LearnerStatsEntity {
    @Id
    @Column(name = "userid")
    private String userId;

    @Column(name = "claps_updated_this_week")
    private Boolean clapsUpdatedThisWeek;

    @Column(name = "last_claps_updated_on")
    private String lastClapsUpdatedOn;

    @Column(name = "last_updated_on")
    private String lastUpdatedOn;

    @Column(name = "total_claps")
    private Integer totalClaps;

    @Column(name = "w1")
    private String w1;

    @Column(name = "w2")
    private String w2;

    @Column(name = "w3")
    private String w3;

    @Column(name = "w4")
    private String w4;

}
