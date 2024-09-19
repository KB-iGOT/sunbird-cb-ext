package org.sunbird.org.service;

import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

public interface OrgDesignationMappingService {
    ResponseEntity<?> getSampleFileForOrgDesignationMapping(String rootOrgId, String userAuthToken, String frameworkId);

    SBApiResponse bulkUploadDesignationMapping(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId);

    void initiateOrgDesignationBulkUploadProcess(String value);

    ResponseEntity<?> downloadFile(String fileName, String rootOrgId, String userAuthToken);

    SBApiResponse getBulkUploadDetailsForOrgDesignationMapping(String orgId, String rootOrgId, String userAuthToken);
}
