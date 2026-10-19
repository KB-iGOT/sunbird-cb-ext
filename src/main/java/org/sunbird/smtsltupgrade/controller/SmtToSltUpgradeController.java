package org.sunbird.smtsltupgrade.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.smtsltupgrade.service.SmtToSltUpgradeService;

/**
 * Exposes an internal endpoint that runs the SMT -> SLT auto-upgrade check.
 * Intended to be triggered by an external Kubernetes CronJob on a schedule (e.g. nightly),
 * not by end users - this is not meant to be exposed via the public-facing proxy/whitelist.
 */
@RestController
@RequestMapping("/v1/smt-slt-upgrade")
public class SmtToSltUpgradeController {


    private SmtToSltUpgradeService smtToSltUpgradeService;

    public SmtToSltUpgradeController(SmtToSltUpgradeService smtToSltUpgradeService) {
        this.smtToSltUpgradeService = smtToSltUpgradeService;
    }

    @PostMapping("/run")
    public ResponseEntity<SBApiResponse> run() {
        SBApiResponse response = smtToSltUpgradeService.runSmtToSltUpgradeCheck();
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
