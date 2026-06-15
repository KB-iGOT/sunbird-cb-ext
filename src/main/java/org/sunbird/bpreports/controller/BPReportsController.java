package org.sunbird.bpreports.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.bpreports.service.BPReportsService;
import org.sunbird.bpreports.service.BPReportsServiceV2;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;

import java.util.Map;


@RequiredArgsConstructor
@RestController
@RequestMapping("/bp")
public class BPReportsController {

    private final BPReportsService bpReportsService;
    private final BPReportsServiceV2 bpReportsServiceV2;

    @PostMapping("/v1/generate/report")
    public ResponseEntity<SBApiResponse> generateBPReport(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = bpReportsService.generateBPReport(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());

    }

    @PostMapping("/v1/bpreport/status")
    public ResponseEntity<?> getBulkUploadDetails(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = bpReportsService.getBPReportStatus(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/bpreport/download/{orgId}/{courseId}/{batchId}/{fileName}")
    public ResponseEntity<?> downloadFile(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @PathVariable("orgId") String orgId, @PathVariable("courseId") String courseId, @PathVariable("batchId") String batchId, @PathVariable("fileName") String fileName) {
        return bpReportsService.downloadBPReport(authToken, orgId, courseId, batchId, fileName);
    }


    @PostMapping("/v2/generate/report")
    public ResponseEntity<SBApiResponse> generateBPReportV2(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = bpReportsServiceV2.generateBPReportV2(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/v2/bpreport/status")
    public ResponseEntity<SBApiResponse> getBPReportStatusV2(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = bpReportsServiceV2.getBPReportStatusV2(requestBody, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
