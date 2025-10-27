package org.sunbird.orghierarchyreport.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.orghierarchyreport.service.OrgHierarchyReportService;

import java.util.Map;

@RestController
public class OrgHierarchyReportController {

    private final OrgHierarchyReportService orgHierarchyReportService;

    public OrgHierarchyReportController(OrgHierarchyReportService orgHierarchyReportService) {
        this.orgHierarchyReportService = orgHierarchyReportService;
    }

    @PostMapping("/orghierarchy/report")
    public ResponseEntity<SBApiResponse> orgExtSearchV3(@RequestBody Map<String, Object> request) {
        SBApiResponse response = orgHierarchyReportService.orgExtSearchV3(request);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
