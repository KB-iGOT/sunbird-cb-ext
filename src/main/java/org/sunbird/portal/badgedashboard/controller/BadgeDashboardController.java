package org.sunbird.portal.badgedashboard.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.portal.badgedashboard.service.BadgeService;

import java.util.Map;

@RestController
public class BadgeDashboardController {

    @Autowired
    BadgeService badgeService;

    @GetMapping("/dashboard/badgedetails/summary")
    public ResponseEntity<Map<String, Object>> getDashboardBadgeDetails() {
        return new ResponseEntity<>(badgeService.getDashboardBadgeDetails(), HttpStatus.OK);
    }
}
