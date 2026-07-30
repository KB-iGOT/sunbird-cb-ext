package org.sunbird.programcoordinator.repository;

import java.util.UUID;

import javax.transaction.Transactional;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorEntity;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorId;

public interface ProgramCoordinatorRepository extends JpaRepository<ProgramCoordinatorEntity, ProgramCoordinatorId> {

    /**
     * Insert a new active coordinator, or resurrect a previously soft-removed one (role_id is
     * overwritten in that case). An already-active row is left untouched by the WHERE guard, which
     * is how the caller tells "added" apart from "already a coordinator" — 0 rows affected means
     * the latter.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = "INSERT INTO program_coordinator (program_id, user_id, role_id, status, created_by, created_on) "
            + "VALUES (:programId, :userId, :roleId, 1, :actorId, now()) "
            + "ON CONFLICT (program_id, user_id) DO UPDATE "
            + "SET role_id = EXCLUDED.role_id, status = 1, created_on = now(), "
            + "updated_by = EXCLUDED.created_by, updated_on = now() "
            + "WHERE program_coordinator.status = 0", nativeQuery = true)
    int addOrResurrect(@Param("programId") String programId, @Param("userId") UUID userId,
            @Param("roleId") Short roleId, @Param("actorId") UUID actorId);

    /**
     * Soft delete. 0 rows affected means the user wasn't an active coordinator to begin with.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = "UPDATE program_coordinator SET status = 0, updated_by = :actorId, updated_on = now() "
            + "WHERE program_id = :programId AND user_id = :userId AND status = 1", nativeQuery = true)
    int softRemove(@Param("programId") String programId, @Param("userId") UUID userId, @Param("actorId") UUID actorId);

    /**
     * Paginated listing, joined to the role master for role_name. Pageable always carries
     * LIMIT/OFFSET; it only carries a Sort when the caller explicitly asked to sort. An unsorted
     * Pageable produces no ORDER BY clause here, so the result falls out in whatever order
     * idx_pc_program_role_active (the only index able to satisfy this WHERE) happens to walk in,
     * without this query ever declaring that as a contract.
     */
    @Query(value = "SELECT pc.user_id AS userId, pc.role_id AS roleId, r.role_name AS roleName "
            + "FROM program_coordinator pc JOIN program_coordinator_role r ON r.id = pc.role_id "
            + "WHERE pc.program_id = :programId AND pc.status = 1",
            countQuery = "SELECT count(*) FROM program_coordinator WHERE program_id = :programId AND status = 1",
            nativeQuery = true)
    Page<ProgramCoordinatorListItem> findCoordinators(@Param("programId") String programId, Pageable pageable);
}
