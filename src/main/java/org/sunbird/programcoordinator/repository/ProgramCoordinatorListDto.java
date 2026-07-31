package org.sunbird.programcoordinator.repository;

import java.util.UUID;

public class ProgramCoordinatorListDto {

    private UUID userId;
    private Short roleId;
    private String roleName;

    public ProgramCoordinatorListDto(UUID userId, Short roleId, String roleName) {
        this.userId = userId;
        this.roleId = roleId;
        this.roleName = roleName;
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

}
