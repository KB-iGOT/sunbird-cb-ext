package org.sunbird.programcoordinator.repository;

import java.util.List;
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
import org.sunbird.programcoordinator.entity.UserProgramProjection;

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
    @Query("SELECT new org.sunbird.programcoordinator.repository.ProgramCoordinatorListDto(" +
            "pc.userId, pc.roleId, r.roleName) " +
            "FROM ProgramCoordinatorEntity pc, ProgramCoordinatorRoleEntity r " +
            "WHERE pc.roleId = r.id " +
            "AND pc.programId = :programId " +
            "AND pc.status = 1")
    Page<ProgramCoordinatorListDto> findCoordinators(
            @Param("programId") String programId,
            Pageable pageable);

    @Query(
            "select pc.programId " +
                    "from ProgramCoordinatorEntity pc " +
                    "where pc.userId = :userId " +
                    "and pc.status = 1"
    )
    List<String> findActiveProgramIdsByUserId(@Param("userId") UUID userId);

    @Query("SELECT pc FROM ProgramCoordinatorEntity pc " +
            "WHERE pc.programId = :programId " +
            "AND pc.status = 1")
    List<ProgramCoordinatorEntity> findActiveByProgramId(
            @Param("programId") String programId);

    @Query("select pc.userId as userId, pc.programId as programId " +
            "from ProgramCoordinatorEntity pc " +
            "where pc.status = 1 " +
            "and pc.userId in :userIds")
    List<UserProgramProjection> findActiveProgramsByUserIds(
            @Param("userIds") List<UUID> userIds);
}
