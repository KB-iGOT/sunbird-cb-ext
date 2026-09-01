package org.sunbird.karmacoinwallet.controller;

import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.karmacoinwallet.service.KarmaCoinWalletService;

@RestController
@RequestMapping("/v1/karmawallet")
public class KarmaCoinWalletController {

    private final KarmaCoinWalletService karmaCoinWalletService;

    public KarmaCoinWalletController(KarmaCoinWalletService karmaCoinWalletService) {
        this.karmaCoinWalletService = karmaCoinWalletService;
    }

    @GetMapping("/summary")
    public ResponseEntity<SBApiResponse> getWalletSummary(@RequestHeader(Constants.X_AUTH_TOKEN) String token) {
        SBApiResponse response = karmaCoinWalletService.getWalletSummary(token);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/transactions")
    public ResponseEntity<SBApiResponse> getTransactions(@RequestHeader(Constants.X_AUTH_TOKEN) String token,
            @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = karmaCoinWalletService.getTransactions(token, requestBody);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/redeem")
    public ResponseEntity<SBApiResponse> redeem(@RequestHeader(Constants.X_AUTH_TOKEN) String token, @RequestBody Map<String, Object> requestBody) {
        SBApiResponse response = karmaCoinWalletService.redeem(token, requestBody);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}