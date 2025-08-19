package org.sunbird.walloffame.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.sunbird.common.util.Constants;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = Constants.TABLE_STATE_TOP_10_LEARNER)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@IdClass(SlwMdoTopLearnerEntity.SlwMdoTopLearnerKey.class)
public class SlwMdoTopLearnerEntity {

    @Id
    @Column(name = "org_id")
    private String orgId;

    @Id
    @Column(name = "row_num")
    private Integer rowNum;

    @Column(name = "designation")
    private String designation;

    @Column(name = "fullname")
    private String fullName;

    @Column(name = "org_name")
    private String orgName;

    @Column(name = "profile_image")
    private String profileImage;

    @Column(name = "total_points")
    private Integer totalPoints;

    @Column(name = "userid")
    private String userId;

    @Column(name = "total_learning_hours")
    private Long totalLearningHours;

    public static class SlwMdoTopLearnerKey implements Serializable {
        private String orgId;
        private Integer rowNum;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SlwMdoTopLearnerKey)) return false;
            SlwMdoTopLearnerKey that = (SlwMdoTopLearnerKey) o;
            return orgId.equals(that.orgId) && rowNum.equals(that.rowNum);
        }

        @Override
        public int hashCode() {
            return orgId.hashCode() + rowNum.hashCode();
        }
    }
}
