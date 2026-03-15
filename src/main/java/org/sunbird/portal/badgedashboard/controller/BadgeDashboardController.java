package org.sunbird.portal.badgedashboard.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.portal.badgedashboard.service.BadgeService;

@RestController
@RequiredArgsConstructor
public class BadgeDashboardController {

    private final BadgeService badgeService;

    @GetMapping("/dashboard/badgedetails/summary")
    public ResponseEntity<SBApiResponse> getDashboardBadgeDetails() {
        SBApiResponse response = badgeService.getDashboardBadgeDetails();
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}
