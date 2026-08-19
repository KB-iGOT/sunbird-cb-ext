package org.sunbird.programcoordinator.entity;

import java.io.Serializable;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Composite primary key for {@link ProgramCoordinatorEntity}: (program_id, user_id).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ProgramCoordinatorId implements Serializable {

    private String programId;
    private UUID userId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ProgramCoordinatorId that = (ProgramCoordinatorId) o;

        if (!programId.equals(that.programId)) return false;
        return userId.equals(that.userId);
    }

    @Override
    public int hashCode() {
        int result = programId.hashCode();
        result = 31 * result + userId.hashCode();
        return result;
    }
}
