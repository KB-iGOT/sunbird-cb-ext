package org.sunbird.insights.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
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
@TypeDef(name = "jsonb", typeClass = JsonBinaryType.class)
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

    @Type(type = "jsonb")
    @Column(name = "w1", columnDefinition = "jsonb")
    private JsonNode w1;

    @Type(type = "jsonb")
    @Column(name = "w2", columnDefinition = "jsonb")
    private JsonNode w2;

    @Type(type = "jsonb")
    @Column(name = "w3", columnDefinition = "jsonb")
    private JsonNode w3;

    @Type(type = "jsonb")
    @Column(name = "w4", columnDefinition = "jsonb")
    private JsonNode w4;
}
