package org.sunbird.org.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

public interface OrgDesignationCompetencyMappingService {
    ResponseEntity<ByteArrayResource> bulkUploadOrganisationCompetencyMapping(String rootOrgId, String userAuthToken, String frameworkId);

    SBApiResponse bulkUploadCompetencyDesignationMapping(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId);

    void initiateCompetencyDesignationBulkUploadProcess(String value);

    ResponseEntity<?> downloadFile(String fileName, String rootOrgId, String userAuthToken);

    SBApiResponse getBulkUploadDetailsForCompetencyDesignationMapping(String orgId, String rootOrgId, String userAuthToken);
}
