package org.sunbird.programcoordinator.service;

import org.sunbird.common.model.SBApiResponse;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;

import java.util.List;

public interface ProgramCoordinatorService {

    /**
     * Adds/reactivates a coordinator (status = 1, roleId required) or soft-removes one
     * (status = 0), depending on the request's status field.
     */
    SBApiResponse upsert(String programId, List<ProgramCoordinatorUpsertRequest> request, String token);

    /**
     * Paginated list of active coordinators for a programme. sortBy/sortDirection are optional;
     * when sortBy is absent or not one of the supported fields, no sort is applied.
     */
    SBApiResponse list(String programId, int limit, int offset, String sortBy, String sortDirection, String token);

    SBApiResponse getProgramCoordinator(String programId, String authUserToken);

    SBApiResponse getCoordinatorRoles(String authUserToken) ;
}
