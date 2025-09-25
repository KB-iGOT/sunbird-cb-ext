package org.sunbird.migrate.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

import java.io.IOException;

public interface UserMigrationService {
    public SBApiResponse migrateUsers();

    ResponseEntity<ByteArrayResource> downloadBulkTransferSampleFile(String rootOrgId, String userAuthToken, String orgHierarchyFrameworkId) throws IOException;

    SBApiResponse bulkUploadUserTransfer(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId, String orgId);

    SBApiResponse getBulkUploadDetailsForOrgDesignationMapping(String orgId, String rootOrgId, String userAuthToken);

    ResponseEntity<?> downloadFile(String fileName, String rootOrgId, String userAuthToken);

}
