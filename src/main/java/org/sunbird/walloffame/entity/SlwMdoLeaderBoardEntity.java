package org.sunbird.walloffame.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.sunbird.common.util.Constants;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = Constants.SLW_MDO_LEADERBOARD)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@IdClass(SlwMdoLeaderBoardEntity.SlwMdoLeaderBoardKey.class)
public class SlwMdoLeaderBoardEntity {
    @Id
    @Column(name = "parent_id")
    private String parentId;

    @Id
    @Column(name = "size")
    private String size;

    @Id
    @Column(name = "row_num")
    private Integer rowNum;

    @Column(name = "org_id")
    private String orgId;

    @Column(name = "org_name")
    private String orgName;

    @Column(name = "total_users")
    private Long totalUsers;

    @Column(name = "total_learning_hours")
    private Long totalLearningHours;



    // Composite key class

    public static class SlwMdoLeaderBoardKey implements Serializable {
        private String parentId;
        private String size;
        private Integer rowNum;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SlwMdoLeaderBoardKey)) return false;
            SlwMdoLeaderBoardKey that = (SlwMdoLeaderBoardKey) o;
            return parentId.equals(that.parentId) &&
                    size.equals(that.size) &&
                    rowNum.equals(that.rowNum);
        }

        @Override
        public int hashCode() {
            return parentId.hashCode() + size.hashCode() + rowNum.hashCode();
        }
    }
}
