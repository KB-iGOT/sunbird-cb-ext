package org.sunbird.walloffame.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.sunbird.common.util.Constants;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Objects;

@Entity
@Table(name = Constants.NLW_MDO_LEADERBOARD)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@IdClass(MdoLeaderboardEntity.MdoLeaderboardKey.class)
public class MdoLeaderboardEntity {
    @Id
    @Column(name = "size")
    private String size;

    @Id
    @Column(name = "row_num")
    private Integer rowNum;

    @Column(name = "last_credit_date")
    private String lastCreditDate;

    @Column(name = "org_id")
    private String orgId;

    @Column(name = "org_name")
    private String orgName;

    @Column(name = "total_points")
    private Long totalPoints;

    @Column(name = "total_users")
    private Long totalUsers;

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MdoLeaderboardKey implements Serializable {
        private static final long serialVersionUID = 1L;

        private String size;
        private Integer rowNum;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof MdoLeaderboardKey)) return false;
            MdoLeaderboardKey that = (MdoLeaderboardKey) o;
            return Objects.equals(size, that.size) &&
                    Objects.equals(rowNum, that.rowNum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(size, rowNum);
        }
    }
}
