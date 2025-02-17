package org.sunbird.nlw.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.nlw.service.PublicUserEventBulkonboardService;

import javax.validation.Valid;
import java.io.IOException;
import java.util.Map;

@RestController
public class PublicUserEventBulkonboardController {

    @Autowired
    PublicUserEventBulkonboardService nlwService;

    @PostMapping("/user/event/bulkOnboard")
    public ResponseEntity<?> processEventUsersForCertificate(@RequestParam(value = "file", required = true) MultipartFile multipartFile, @RequestParam(value = "eventId", required = true) String eventId, @RequestParam(value = "batchId", required = true) String batchId, @RequestParam(value = "publicCert", required = false) String publicCert, @RequestParam(value = "reissue", required = false) String reissue) throws IOException {
        SBApiResponse uploadResponse = nlwService.bulkOnboard(multipartFile, eventId, batchId, publicCert, reissue);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());

    }
    @GetMapping("/user/event/bulkonboard/status/{eventId}")
    public ResponseEntity<?> getUserEventBulkOnboardDetails(@PathVariable("eventId") String eventId) {
        SBApiResponse response = nlwService.getUserEventBulkOnboardDetails(eventId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/user/event/bulkonboard/download/{fileName}")
    public ResponseEntity<?> downloadFile(@PathVariable("fileName") String fileName) {
        return nlwService.downloadFile(fileName);
    }

    @PostMapping("/v2/user/event/bulkOnboard")
    public ResponseEntity<?> processEventUsersForCertificateV2(@RequestParam(value = "file", required = true) MultipartFile multipartFile, @Valid @RequestBody Map<String, Object> requestBody ) {
        String eventId = getStringValue(requestBody, Constants.EVENT_ID);
        String batchId = getStringValue(requestBody, Constants.BATCH_ID);
        String publicCert = getStringValue(requestBody, Constants.PUBLIC_CERT);
        String reissue = getStringValue(requestBody, Constants.REISSUE);
        SBApiResponse uploadResponse = nlwService.bulkOnboard(multipartFile, eventId, batchId, publicCert, reissue);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());

    }

    private String getStringValue(Map<String, Object> map, String key) {
        return map.keySet().stream()
                .filter(k -> k.equals(key))
                .map(k -> (String) map.get(k))
                .findFirst()
                .orElse(null);  // Return null if the key is not found
    }
}
