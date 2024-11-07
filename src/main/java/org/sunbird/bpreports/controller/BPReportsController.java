package org.sunbird.bpreports.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.bpreports.service.BPReportsService;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;

import java.util.Map;


@RestController
@RequestMapping("/bp")
public class BPReportsController {

    @Autowired
    private BPReportsService bpReportsService;

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
}
