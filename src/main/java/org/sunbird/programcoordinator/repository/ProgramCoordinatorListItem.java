package org.sunbird.programcoordinator.repository;

import java.util.UUID;

/**
 * Interface projection for the list API — column aliases in
 * {@link ProgramCoordinatorRepository#findCoordinators} must match these getter names.
 */
public interface ProgramCoordinatorListItem {
    UUID getUserId();
    Short getRoleId();
    String getRoleName();
}
