package org.sunbird.programcoordinator.dto;

import java.util.UUID;

import javax.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;

/**
 * Body for the upsert API. Exactly one of roleId/roleName is required only when status = 1
 * (add/reactivate); both are ignored when status = 0 (remove). roleId is kept for backward
 * compatibility with existing callers; new callers pass the human-readable roleName (e.g.
 * "National Lead Trainer") instead, which the service layer resolves against the
 * program_coordinator_role table. Validated in the service layer since these conditions can't be
 * expressed with plain bean-validation annotations. isCoTrainer is optional and defaults to
 * false when omitted.
 */
@Getter
@Setter
public class ProgramCoordinatorUpsertRequest {

    @NotNull
    private UUID userId;

    private Short roleId;

    private String roleName;

    @NotNull
    private Short status;

    private Boolean isCoTrainer;
}
