package org.sunbird.programcoordinator.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.service.ProgramCoordinatorService;
import java.util.List;

import static org.sunbird.common.util.Constants.PROGRAM_ID;
import static org.sunbird.common.util.Constants.X_AUTH_TOKEN;

@RestController
public class ProgramCoordinatorController {

    private final ProgramCoordinatorService programCoordinatorService;

    public ProgramCoordinatorController(ProgramCoordinatorService programCoordinatorService) {
        this.programCoordinatorService = programCoordinatorService;
    }

    /**
     * Adds/reactivates a coordinator (status = 1) or soft-removes one (status = 0) on a programme.
     */
    @PutMapping("/program/{programId}/coordinator")
    public ResponseEntity<?> upsertCoordinator(@PathVariable(PROGRAM_ID) String programId,
            @RequestHeader(X_AUTH_TOKEN) String token,
            @Valid @RequestBody List<ProgramCoordinatorUpsertRequest> requestBody) {
        SBApiResponse response = programCoordinatorService.upsert(programId, requestBody, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Paginated list of active coordinators for a programme. sortBy/sortDirection are optional;
     * when omitted (or sortBy isn't a supported field), no sort is applied to the query.
     */
    @GetMapping("/program/{programId}/coordinator")
    public ResponseEntity<?> listCoordinators(@PathVariable(PROGRAM_ID) String programId,
            @RequestParam(name = "limit", defaultValue = "20") int limit,
            @RequestParam(name = "offset", defaultValue = "0") int offset,
            @RequestParam(name = "sortBy", required = false) String sortBy,
            @RequestParam(name = "sortDirection", required = false) String sortDirection, @RequestHeader(X_AUTH_TOKEN) String token) {
        SBApiResponse response = programCoordinatorService.list(programId, limit, offset, sortBy, sortDirection, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }


    @GetMapping("/v1/program/{programId}/coordinators")
    public ResponseEntity<?> getProgramCoordinators(
            @PathVariable("programId") String programId, @RequestHeader(X_AUTH_TOKEN) String authUserToken) {

        SBApiResponse response = programCoordinatorService.getProgramCoordinator(programId, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/program/coordinator/roles")
    public ResponseEntity<?> getCoordinatorRoles(@RequestHeader(X_AUTH_TOKEN) String authUserToken) {

        SBApiResponse response = programCoordinatorService.getCoordinatorRoles(authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}
