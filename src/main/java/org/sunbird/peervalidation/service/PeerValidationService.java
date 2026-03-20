package org.sunbird.peervalidation.service;

import org.sunbird.common.model.SBApiResponse;


public interface PeerValidationService {

    /**
     * Initialize a download request for form submissions
     * @param formId - form identifier
     * @param authUserToken - authentication token to extract userId
     * @return SBApiResponse with download request details
     */
    SBApiResponse initReportDownload(String formId, String authUserToken);

    /**
     * List all download requests for a given form and organization
     * @param authUserToken - authentication token to extract userId
     * @return SBApiResponse with list of download requests
     */
    SBApiResponse listReportDownloads(String authUserToken);
}
