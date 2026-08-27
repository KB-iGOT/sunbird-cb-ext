package org.sunbird.karmacoinwallet.service;

import java.util.Map;

import org.sunbird.common.model.SBApiResponse;

public interface KarmaCoinWalletService {

    SBApiResponse getWalletSummary(String token);

    SBApiResponse getTransactions(String token, Map<String, Object> requestBody);
}