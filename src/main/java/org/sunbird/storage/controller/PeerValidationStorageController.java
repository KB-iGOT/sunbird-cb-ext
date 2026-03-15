package org.sunbird.storage.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.storage.service.PeerValidationFileService;

import java.io.IOException;

@RestController
@RequestMapping("peersurvey")
public class PeerValidationStorageController {

    @Autowired
    PeerValidationFileService  peerValidationFileService;

    @PostMapping("/upload")
    public ResponseEntity<?> uploadPeerValidationFile(
            @RequestParam(Constants.FILE) MultipartFile multipartFile,
            @RequestParam(Constants.FORM_ID) String formId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String userToken) {

        SBApiResponse response = peerValidationFileService.uploadPeerValidationFile(multipartFile, formId, userToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
