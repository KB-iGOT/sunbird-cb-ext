package org.sunbird.org.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

import java.io.IOException;

public interface OrgHierarchyService {

    ResponseEntity<ByteArrayResource> bulkUploadOrganisationMapping(String rootOrgId, String userAuthToken, String frameworkId);

    ResponseEntity<ByteArrayResource> exportOrgHierarchyToExcel(String userAuthToken, String frameworkId);

    SBApiResponse bulkUploadOrgHierarchyMapping(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId);
}
