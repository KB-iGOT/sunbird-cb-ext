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

@RestController
public class ProgramCoordinatorController {

    @Autowired
    private ProgramCoordinatorService programCoordinatorService;

    /**
     * Adds/reactivates a coordinator (status = 1) or soft-removes one (status = 0) on a programme.
     */
    @PutMapping("/program/{programId}/coordinator")
    public ResponseEntity<?> upsertCoordinator(@PathVariable("programId") String programId,
            @RequestHeader("x-authenticated-userid") String userId,
            @Valid @RequestBody ProgramCoordinatorUpsertRequest requestBody) throws Exception {
        SBApiResponse response = programCoordinatorService.upsert(programId, requestBody, userId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Paginated list of active coordinators for a programme. sortBy/sortDirection are optional;
     * when omitted (or sortBy isn't a supported field), no sort is applied to the query.
     */
    @GetMapping("/program/{programId}/coordinator")
    public ResponseEntity<?> listCoordinators(@PathVariable("programId") String programId,
            @RequestParam(name = "limit", defaultValue = "20") int limit,
            @RequestParam(name = "offset", defaultValue = "0") int offset,
            @RequestParam(name = "sortBy", required = false) String sortBy,
            @RequestParam(name = "sortDirection", required = false) String sortDirection) throws Exception {
        SBApiResponse response = programCoordinatorService.list(programId, limit, offset, sortBy, sortDirection);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
