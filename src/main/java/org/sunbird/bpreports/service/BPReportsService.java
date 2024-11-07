package org.sunbird.bpreports.service;

import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface BPReportsService {

    SBApiResponse generateBPReport(Map<String, Object> requestBody, String authToken);

    SBApiResponse getBPReportStatus(Map<String, Object> requestBody, String authToken);

    ResponseEntity<Resource> downloadBPReport(String authToken, String orgId, String courseId, String batchId, String fileName);
}
