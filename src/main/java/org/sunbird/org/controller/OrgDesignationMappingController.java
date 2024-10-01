package org.sunbird.org.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.org.service.OrgDesignationMappingService;

@RestController
@RequestMapping("/designation")
public class OrgDesignationMappingController {

    @Autowired
    OrgDesignationMappingService orgDesignationMappingService;

    @GetMapping("/v1/orgMapping/sample/{frameworkId}")
    public ResponseEntity<?> getCompetencyMappingFile(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                      @PathVariable(Constants.FRAMEWORK_ID) String frameworkId,
                                                      @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        return orgDesignationMappingService.getSampleFileForOrgDesignationMapping(rootOrgId, userAuthToken, frameworkId);
    }

    @PostMapping("/v1/orgMapping/bulkUpload/{frameworkId}")
    public ResponseEntity<?> bulkUploadCompetencyDesignationMapping(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                                    @RequestParam(value = "file", required = true) MultipartFile file,
                                                                    @PathVariable(Constants.FRAMEWORK_ID) String frameworkId,
                                                                    @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        SBApiResponse uploadResponse = orgDesignationMappingService.bulkUploadDesignationMapping(file, rootOrgId, userAuthToken, frameworkId);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());
    }

    @GetMapping("/v1/orgMapping/progress/details/bulkUpload/{orgId}")
    public ResponseEntity<?> getBulkUploadDetails(@PathVariable(Constants.ORG_ID) String orgId,
                                                  @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                  @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        SBApiResponse response = orgDesignationMappingService.getBulkUploadDetailsForOrgDesignationMapping(orgId, rootOrgId, userAuthToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/orgMapping/download/{fileName}")
    public ResponseEntity<?> downloadFile(@PathVariable(Constants.FILE_NAME) String fileName,
                                          @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                          @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        return orgDesignationMappingService.downloadFile(fileName, rootOrgId, userAuthToken);
    }
}
