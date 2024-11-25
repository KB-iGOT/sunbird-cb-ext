package org.sunbird.customselfregistration.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.customselfregistration.service.CustomSelfRegistrationService;

import javax.validation.Valid;
import java.util.Map;

/**
 * @author mahesh.vakkund
 */
@RestController
public class CustomSelfRegistrationController {

    @Autowired
    CustomSelfRegistrationService customSelfRegistrationService;

    @PostMapping(value = "/getSelfRegistrationQRPdf", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SBApiResponse> getBatchSessionQRPdf(@RequestHeader("x-authenticated-user-token") String authUserToken, @Valid @RequestBody Map<String, Object> requestBody) {
        return new ResponseEntity<>(customSelfRegistrationService.getSelfRegistrationQRAndLink(authUserToken,requestBody), HttpStatus.OK);
    }

    @PostMapping(value = "/listAllQRCodes", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SBApiResponse> getAllRegistrationQRCodesList(@RequestHeader("x-authenticated-user-token") String authUserToken, @Valid @RequestBody Map<String, Object> requestBody) {
        return new ResponseEntity<>(customSelfRegistrationService.getAllRegistrationQRCodesList(authUserToken,requestBody), HttpStatus.OK);
    }

    @PostMapping(value = "/expiredQRCodes", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<SBApiResponse> expireRegistrationQRCodes(@Valid @RequestBody Map<String, Object> requestBody) {
        return new ResponseEntity<>(customSelfRegistrationService.expireRegistrationQRCodes(requestBody), HttpStatus.OK);
    }
}
