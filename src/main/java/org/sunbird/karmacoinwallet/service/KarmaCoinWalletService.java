package org.sunbird.karmacoinwallet.service;

import org.sunbird.common.model.SBApiResponse;

public interface KarmaCoinWalletService {

    SBApiResponse getWalletSummary(String token);
}