package org.sunbird.programcoordinator.repository;

import java.util.UUID;

public class ProgramCoordinatorListDto {

    private UUID userId;
    private Short roleId;
    private String roleName;
    private UUID createdBy;

    public ProgramCoordinatorListDto(UUID userId, Short roleId, String roleName, UUID createdBy) {
        this.userId = userId;
        this.roleId = roleId;
        this.roleName = roleName;
        this.createdBy = createdBy;
    }

    public UUID getUserId() {
        return userId;
    }

    public Short getRoleId() {
        return roleId;
    }

    public String getRoleName() {
        return roleName;
    }


    public UUID getCreatedBy() {
        return createdBy;
    }
}
