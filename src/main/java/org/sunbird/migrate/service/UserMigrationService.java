package org.sunbird.migrate.service;

import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.ResponseEntity;
import org.sunbird.common.model.SBApiResponse;

import java.io.IOException;

public interface UserMigrationService {
    public SBApiResponse migrateUsers();

    ResponseEntity<ByteArrayResource> downloadBulkTransferSampleFile(String rootOrgId, String userAuthToken) throws IOException;

}
