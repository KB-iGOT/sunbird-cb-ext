package org.sunbird.programcoordinator.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorRoleEntity;

/**
 * Read-only access to the role master, used to validate an incoming role_id before it hits the
 * program_coordinator FK constraint.
 */
public interface ProgramCoordinatorRoleRepository extends JpaRepository<ProgramCoordinatorRoleEntity, Short> {
}
