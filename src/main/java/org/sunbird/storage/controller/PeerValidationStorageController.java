package org.sunbird.storage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.storage.service.PeerValidationFileService;

import java.io.IOException;

@RestController
@RequestMapping("peersurvey")
public class PeerValidationStorageController {

    @Autowired
    PeerValidationFileService  peerValidationFileService;

    @PostMapping("/upload")
    public ResponseEntity<?> uploadPeerValidationFile(
            @RequestParam("file") MultipartFile multipartFile,
            @RequestParam("formId") String formId,
            @RequestParam("userId") String userId) {

        SBApiResponse response = peerValidationFileService.uploadPeerValidationFile(multipartFile, formId, userId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
