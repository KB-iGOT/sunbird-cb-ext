package org.sunbird.programcoordinator.controller;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.service.ProgramCoordinatorBulkUploadService;
import org.sunbird.programcoordinator.service.ProgramCoordinatorService;
import java.util.List;
import java.util.Map;

import static org.sunbird.common.util.Constants.PROGRAM_ID;
import static org.sunbird.common.util.Constants.X_AUTH_TOKEN;

@RestController
public class ProgramCoordinatorController {

    private final ProgramCoordinatorService programCoordinatorService;
    private final ProgramCoordinatorBulkUploadService programCoordinatorBulkUploadService;

    public ProgramCoordinatorController(ProgramCoordinatorService programCoordinatorService,
            ProgramCoordinatorBulkUploadService programCoordinatorBulkUploadService) {
        this.programCoordinatorService = programCoordinatorService;
        this.programCoordinatorBulkUploadService = programCoordinatorBulkUploadService;
    }

    /**
     * Adds/reactivates a coordinator (status = 1) or soft-removes one (status = 0) on a programme.
     */
    @PutMapping("/program/coordinator/{programId}")
    public ResponseEntity<?> upsertCoordinator(@PathVariable(PROGRAM_ID) String programId,
            @RequestHeader(X_AUTH_TOKEN) String token,
            @Valid @RequestBody List<ProgramCoordinatorUpsertRequest> requestBody) {
        SBApiResponse response = programCoordinatorService.upsert(programId, requestBody, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/program/coordinator/list/{programId}")
    public ResponseEntity<?> listCoordinators(
            @PathVariable(PROGRAM_ID) String programId,
            @RequestBody Map<String, Object> request,
            @RequestHeader(X_AUTH_TOKEN) String token) {

        SBApiResponse response = programCoordinatorService.list(
                programId,
                request,
                token);

        return new ResponseEntity<>(response, response.getResponseCode());
    }


    @GetMapping("/v1/program/{programId}/coordinators")
    public ResponseEntity<SBApiResponse> getProgramCoordinators(
            @PathVariable("programId") String programId, @RequestHeader(X_AUTH_TOKEN) String authUserToken) {

        SBApiResponse response = programCoordinatorService.getProgramCoordinator(programId, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/program/coordinator/roles")
    public ResponseEntity<SBApiResponse> getCoordinatorRoles(@RequestHeader(X_AUTH_TOKEN) String authUserToken) {

        SBApiResponse response = programCoordinatorService.getCoordinatorRoles(authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PutMapping("/admin/program/coordinator/upsert/{programId}")
    public ResponseEntity<SBApiResponse> upsertByAdmin(
            @PathVariable(PROGRAM_ID) String programId,
            @RequestBody List<ProgramCoordinatorUpsertRequest> requests,
            @RequestHeader(X_AUTH_TOKEN) String token) {

        SBApiResponse response = programCoordinatorService.upsertByAdmin(programId, requests, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * Bulk "Program Coordinator" addition: uploads a CSV/Excel file of (Registered Name,
     * Registered Email ID, Trainer Type) rows. Validates and queues the file for async
     * processing; poll getBulkUploadStatus for the outcome.
     */
    @PostMapping("/program/coordinator/bulk-upload/{programId}")
    public ResponseEntity<SBApiResponse> bulkUploadCoordinators(
            @PathVariable(PROGRAM_ID) String programId,
            @RequestParam("file") MultipartFile file,
            @RequestHeader(X_AUTH_TOKEN) String token) {

        SBApiResponse response = programCoordinatorBulkUploadService.bulkUpload(programId, file, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/program/coordinator/bulk-upload/{programId}/status/{identifier}")
    public ResponseEntity<SBApiResponse> getBulkUploadStatus(
            @PathVariable(PROGRAM_ID) String programId,
            @PathVariable("identifier") String identifier,
            @RequestHeader(X_AUTH_TOKEN) String token) {

        SBApiResponse response = programCoordinatorBulkUploadService.getStatus(programId, identifier, token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}
