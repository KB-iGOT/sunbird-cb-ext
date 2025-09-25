package org.sunbird.migrate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.migrate.service.UserMigrationService;

import java.io.IOException;

@RestController
@RequestMapping("/user/migration")
public class UserMigrationController {

    @Autowired
    private UserMigrationService userMigrationService;

    @GetMapping("/initiate")
    public ResponseEntity<?> initiateUserMigration() throws Exception {

        SBApiResponse response = userMigrationService.migrateUsers();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/getMappingFile/sample/{orgHierarchyFrameworkId}")
    public ResponseEntity<?> getSampleMappingFileBulkUpload(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId, @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken, @PathVariable(Constants.ORG_HIERARCHY_FRAMEWORK_ID_KEY) String orgHierarchyFrameworkId) throws IOException {

        return userMigrationService.downloadBulkTransferSampleFile(rootOrgId, userAuthToken, orgHierarchyFrameworkId);
    }

    @PostMapping("/v1/hierarchy/bulkUpload/{frameworkId}")
    public ResponseEntity<?> bulkUploadOrgHierarchyMapping(@RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                           @RequestParam(value = "file") MultipartFile file,
                                                           @PathVariable(Constants.FRAMEWORK_ID) String frameworkId,
                                                           @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken,
                                                           @RequestParam(value = Constants.ORG_ID) String orgId) {

        SBApiResponse uploadResponse = userMigrationService.bulkUploadUserTransfer(file, rootOrgId, userAuthToken, frameworkId, orgId);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());

    }

    @GetMapping("/v1/orgMapping/progress/details/bulkUpload/{orgId}")
    public ResponseEntity<?> getBulkUploadDetails(@PathVariable(Constants.ORG_ID) String orgId,
                                                  @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                                  @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        SBApiResponse response = userMigrationService.getBulkUploadDetailsForOrgDesignationMapping(orgId, rootOrgId, userAuthToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/orgMapping/download/{fileName}")
    public ResponseEntity<?> downloadFile(@PathVariable(Constants.FILE_NAME) String fileName,
                                          @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
                                          @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) {

        return userMigrationService.downloadFile(fileName, rootOrgId, userAuthToken);
    }

}
