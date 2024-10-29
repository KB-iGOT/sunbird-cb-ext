package org.sunbird.bpreports.service;

import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface BPReportsService {

    public SBApiResponse generateBPReport(Map<String, Object> requestBody, String authToken);
    public SBApiResponse getBPReportStatus(Map<String, Object> requestBody);
    public ResponseEntity<Resource> downloadBPReport(String authToken, String fileName);
}
