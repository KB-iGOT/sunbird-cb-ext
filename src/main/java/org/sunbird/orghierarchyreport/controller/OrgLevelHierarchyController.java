package org.sunbird.orghierarchyreport.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.orghierarchyreport.service.OrgLevelHierarchyService;

import java.util.Map;

@RestController
public class OrgLevelHierarchyController {

    private final OrgLevelHierarchyService orgLevelHierarchyService;

    public OrgLevelHierarchyController(OrgLevelHierarchyService orgLevelHierarchyService) {
        this.orgLevelHierarchyService = orgLevelHierarchyService;
    }

    @PostMapping("/org/level/hierarchy")
    public ResponseEntity<SBApiResponse> orgExtSearchV3(@RequestBody Map<String, Object> request) {
        SBApiResponse response = orgLevelHierarchyService.orgExtSearchV3(request);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
