package org.sunbird.nlw.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.nlw.service.PublicUserEventBulkonboardService;

import java.io.IOException;

@RestController
public class PublicUserEventBulkonboardController {

    @Autowired
    PublicUserEventBulkonboardService nlwService;

    @PostMapping("/user/event/bulkOnboard")
    public ResponseEntity<?> processEventUsersForCertificate(@RequestParam(value = "file", required = true) MultipartFile multipartFile, @RequestParam(value = "eventId", required = true) String eventId, @RequestParam(value = "batchId", required = true) String batchId) throws IOException {
        SBApiResponse uploadResponse = nlwService.bulkOnboard(multipartFile, eventId, batchId);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());

    }
}
