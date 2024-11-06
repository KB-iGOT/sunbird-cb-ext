package org.sunbird.customselfregistration.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

/**
 * @author mahesh.vakkund
 */
public interface CustomSelfRegistrationService {

    /**
     * Generates a self-registration QR code PDF for the organisation.
     *
     * @param authUserToken The authentication token of the user requesting the QR code.
     * @param requestBody   A map containing the request body parameters.
     *
     * @return An SBApiResponse object containing the response of processing the request.
     */
    SBApiResponse getSelfRegistrationQRAndLink(String authUserToken, Map<String, Object> requestBody);
}
