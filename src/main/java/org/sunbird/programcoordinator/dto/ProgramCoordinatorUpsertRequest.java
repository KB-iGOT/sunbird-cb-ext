package org.sunbird.programcoordinator.dto;

import java.util.UUID;

import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;

/**
 * Body for the upsert API. roleId is required only when status = 1 (add/reactivate); it is
 * ignored when status = 0 (remove). Validated in the service layer since that condition can't be
 * expressed with a plain bean-validation annotation.
 */
@Getter
@Setter
public class ProgramCoordinatorUpsertRequest {

    @NotNull
    private UUID userId;

    private Short roleId;

    @NotNull
    private Short status;
}
