package org.sunbird.peervalidation.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.peervalidation.service.PeerValidationService;

@RestController
@RequestMapping("/peerValidation")
public class PeerValidationController {

    @Autowired
    private PeerValidationService peerValidationService;

    /**
     * Initialize a download request for form submissions
     * GET /api/v1/report-downloads/init/{formId}
     *
     * @param formId - form identifier (required)
     * @param authUserToken - authentication token from x-authenticated-user-token header
     * @return ResponseEntity with download request details
     */
    @GetMapping("/v1/report/initiate/{formId}")
    public ResponseEntity<SBApiResponse> reportInit(
            @PathVariable("formId") String formId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String authUserToken) {

        SBApiResponse response = peerValidationService.initReportDownload(formId, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    /**
     * List all download requests for a given form and organization
     * GET /api/v1/report-downloads/{formId}
     *
     * @param authUserToken - authentication token from x-authenticated-user-token header
     * @return ResponseEntity with list of download requests
     */
    @GetMapping("/v1/list/report")
    public ResponseEntity<SBApiResponse> reportList(
            @RequestHeader(Constants.X_AUTH_TOKEN) String authUserToken) {

        SBApiResponse response = peerValidationService.listReportDownloads(authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}


