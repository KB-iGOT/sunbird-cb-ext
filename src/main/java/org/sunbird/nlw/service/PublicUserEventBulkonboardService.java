package org.sunbird.nlw.service;

import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

public interface PublicUserEventBulkonboardService {

    SBApiResponse bulkOnboard(MultipartFile multipartFile, String eventId, String batchId);
    ResponseEntity<Resource> downloadFile(String fileName);
    SBApiResponse getUserEventBulkOnboardDetails(String eventId);
}
