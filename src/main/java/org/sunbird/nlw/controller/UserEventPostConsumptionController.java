package org.sunbird.nlw.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.nlw.service.UserEventPostConsumptionService;

import java.io.IOException;

@RestController
public class UserEventPostConsumptionController {

    @Autowired
    UserEventPostConsumptionService nlwService;

    @PostMapping("/user/event/postConsumption")
    public ResponseEntity<?> processEventUsersForCertificateAndKarmaPoints(@RequestParam(value = "file", required = true) MultipartFile multipartFile) throws IOException {
        SBApiResponse uploadResponse = nlwService.processEventUsersForCertificateAndKarmaPoints(multipartFile);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());
    }

    @PostMapping("/user/event/postConsumption/updateStatus")
    public ResponseEntity<?> processEventUsersForStatus(@RequestParam(value = "file", required = true) MultipartFile multipartFile) throws IOException {
        SBApiResponse uploadResponse = nlwService.processEventUsersForStatus(multipartFile);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());
    }
}
