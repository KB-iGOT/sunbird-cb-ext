package org.sunbird.migrate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.migrate.service.UserMigrationService;

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
}
