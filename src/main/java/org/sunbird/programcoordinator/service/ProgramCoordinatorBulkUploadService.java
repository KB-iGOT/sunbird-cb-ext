package org.sunbird.programcoordinator.service;

import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

/**
 * Bulk "Program Coordinator" addition: a Program Coordinator uploads a CSV/Excel file of
 * (Registered Name, Registered Email ID, Trainer Type) rows. Structural validation, upload and
 * tracking happen synchronously here; per-row processing (registration check, role assignment,
 * profile update, coordinator upsert) happens asynchronously in
 * ProgramCoordinatorBulkUploadProcessingService.
 */
public interface ProgramCoordinatorBulkUploadService {

    /**
     * Entry point for the bulk upload request. Validates access and file structure, uploads the
     * raw file, persists a tracking record and publishes a Kafka event for async processing. The
     * uploader's id (recorded as createdBy) is derived from userAuthToken, same as
     * ProgramCoordinatorServiceImpl.upsert() derives its actor id - no separate id header needed.
     *
     * @param programId     target programme
     * @param file          uploaded CSV/XLSX of coordinator rows
     * @param userAuthToken caller's auth token
     */
    SBApiResponse bulkUpload(String programId, MultipartFile file, String userAuthToken);

    /**
     * Reads back the tracking record for a previously submitted bulk upload.
     */
    SBApiResponse getStatus(String programId, String identifier, String userAuthToken);

    /**
     * Lists every bulk upload job ever submitted for this programme (not just one by identifier) -
     * a history/log view, same shape as ProfileServiceImpl.getBulkUploadDetails(orgId) does for
     * the older govt user bulk-upload flow.
     */
    SBApiResponse getBulkUploadList(String programId, String userAuthToken);

    /**
     * Downloads an uploaded or result file by name, proxying it through this service rather than
     * handing back the raw cloud storage URL - same approach as
     * ProfileServiceImpl.downloadFile(fileName) for the older govt user bulk-upload flow.
     */
    ResponseEntity<Resource> downloadFile(String fileName);
}
