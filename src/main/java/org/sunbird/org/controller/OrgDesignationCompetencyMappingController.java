package org.sunbird.org.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.org.service.OrgDesignationCompetencyMappingService;

@RestController
@RequestMapping("/organisation")
public class OrgDesignationCompetencyMappingController {

    @Autowired
    OrgDesignationCompetencyMappingService orgDesignationCompetencyMappingService;

    @GetMapping("/v1/getCompetencyMappingFile/sample/{frameworkId}")
    public ResponseEntity<?> getSampleCompetencyMappingFileBulkUpload(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                                      @PathVariable(Constants.FRAMEWORK_ID) String frameworkId,
                                                                      @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        return orgDesignationCompetencyMappingService.bulkUploadOrganisationCompetencyMapping(rootOrgId, userAuthToken, frameworkId);
    }

    @PostMapping("/v1/competencyDesignationMappings/bulkUpload/{frameworkId}")
    public ResponseEntity<?> bulkUploadCompetencyDesignationMapping(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                                    @RequestParam(value = "file", required = true) MultipartFile file,
                                                                    @PathVariable(Constants.FRAMEWORK_ID) String frameworkId,
                                                                    @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        SBApiResponse uploadResponse = orgDesignationCompetencyMappingService.bulkUploadCompetencyDesignationMapping(file, rootOrgId, userAuthToken, frameworkId);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());
    }

    @GetMapping("/v1/competencyDesignationMappings/progress/details/bulkUpload/{orgId}")
    public ResponseEntity<?> getBulkUploadDetails(@PathVariable(Constants.ORG_ID) String orgId,
                                                  @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                  @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        SBApiResponse response = orgDesignationCompetencyMappingService.getBulkUploadDetailsForCompetencyDesignationMapping(orgId, rootOrgId, userAuthToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/competencyDesignationMappings/download/{fileName}")
    public ResponseEntity<?> downloadFile(@PathVariable(Constants.FILE_NAME) String fileName,
                                          @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                          @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        return orgDesignationCompetencyMappingService.downloadFile(fileName, rootOrgId, userAuthToken);
    }
}
