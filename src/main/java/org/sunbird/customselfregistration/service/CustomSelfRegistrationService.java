package org.sunbird.customselfregistration.service;

import org.springframework.web.multipart.MultipartFile;
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

    /**
     * Retrieves the list of all registration QR codes.
     *
     * @param authUserToken The authentication token of the user making the request.
     * @param requestBody   The request body containing any additional parameters required for the request.
     * @return SBApiResponse containing the list of registration QR codes or an error message if the request fails.
     */
    SBApiResponse getAllRegistrationQRCodesList(String authUserToken,  Map<String, Object> requestBody);

    /**
     * Expire registration QR codes for a given organization ID.
     *
     * @param requestBody The request body containing the organization ID.
     * @return The API response with the updated organization IDs and QR code IDs.
     */
    SBApiResponse expireRegistrationQRCodes(Map<String, Object> requestBody);

    /**
     * Uploads an image to a Google Cloud Platform (GCP) container.
     *
     * @param multipartFile the image file to be uploaded
     * @param authUserToken the authentication token for the user
     * @return the response object containing the result of the upload operation
     */
    SBApiResponse uploadImageToGCPContainer(MultipartFile multipartFile, String authUserToken);

    /**
     * Checks if the registration QR code is active for the given request.
     *
     * @param requestBody A map containing the request body parameters.
     * @return SBApiResponse containing the result of the request or an error message if the request fails.
     */
    SBApiResponse isRegistrationQRActive(Map<String, Object> requestBody);
}
