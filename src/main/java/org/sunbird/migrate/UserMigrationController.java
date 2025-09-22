package org.sunbird.migrate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
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


}
