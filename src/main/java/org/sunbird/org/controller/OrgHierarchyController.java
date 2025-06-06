package org.sunbird.org.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.util.Constants;
import org.sunbird.org.service.OrgHierarchyService;

@RestController
@RequestMapping("/organisation")
public class OrgHierarchyController {


    private @Autowired OrgHierarchyService orgHierarchyService;

    @GetMapping("/v1/getMappingFile/sample/{frameworkId}")
    public ResponseEntity<?> getSampleMappingFileBulkUpload(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                                      @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken,
                                                            @PathVariable(Constants.FRAMEWORK_ID) String frameworkId) {

        return orgHierarchyService.bulkUploadOrganisationMapping(rootOrgId, userAuthToken, frameworkId);
    }


}