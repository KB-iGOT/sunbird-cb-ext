package org.sunbird.programcoordinator.repository;

import java.util.Date;
import java.util.UUID;

public class ProgramCoordinatorListDto {

    private UUID userId;
    private Short roleId;
    private String roleName;
    private UUID createdBy;
    private Date createdOn;
    private Date updatedOn;

    public ProgramCoordinatorListDto(UUID userId, Short roleId, String roleName, UUID createdBy,
            Date createdOn, Date updatedOn) {
        this.userId = userId;
        this.roleId = roleId;
        this.roleName = roleName;
        this.createdBy = createdBy;
        this.createdOn = createdOn;
        this.updatedOn = updatedOn;
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

    public Date getCreatedOn() {
        return createdOn;
    }

    public Date getUpdatedOn() {
        return updatedOn;
    }
}
