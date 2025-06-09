package org.sunbird.org.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.ResponseEntity;

import java.io.IOException;

public interface OrgHierarchyService {

    ResponseEntity<ByteArrayResource> bulkUploadOrganisationMapping(String rootOrgId, String userAuthToken, String frameworkId);

    ResponseEntity<ByteArrayResource> exportOrgHierarchyToExcel(String userAuthToken, String frameworkId);
}
