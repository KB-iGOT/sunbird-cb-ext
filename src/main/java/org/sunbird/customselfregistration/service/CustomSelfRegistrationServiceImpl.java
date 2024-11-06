package org.sunbird.customselfregistration.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.glxn.qrgen.core.image.ImageType;
import net.glxn.qrgen.javase.QRCode;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.storage.service.StorageServiceImpl;
import org.sunbird.workallocation.service.PdfGeneratorServiceImpl;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Implementation of the CustomSelfRegistrationService interface.
 * <p>
 * This class provides the business logic for custom self-registration functionality.
 * It handles requests related to self-registration, such as generating QR codes and the registration links.
 * *
 *
 * @author mahesh.vakkund
 */
@Service
public class CustomSelfRegistrationServiceImpl implements CustomSelfRegistrationService {
    private final Logger logger = LoggerFactory.getLogger(CustomSelfRegistrationServiceImpl.class);

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    StorageServiceImpl storageService;

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    PdfGeneratorServiceImpl pdfGeneratorService;

    /**
     * Generates a self-registration QR code and link for the organisation.
     *
     * @param authUserToken The authentication token of the user requesting the QR code and link.
     * @param requestBody   A map containing the request body parameters.
     * @return An SBApiResponse object.
     */
    @Override
    public SBApiResponse getSelfRegistrationQRAndLink(String authUserToken, Map<String, Object> requestBody) {
        logger.info("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRPdf.. started");
        // Create a default response object
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CUSTOM_SELF_REGISTRATION_CREATE_API);
        String qrCodeBody = "";
        // Create a QR code body
        HashMap<String, Object> qrBody = new HashMap<>();
        HashMap<String, HashMap<String, String>> pdfDetails = populatePDFTemplateDetails();
        HashMap<String, HashMap> pdfParams = populatePDFParams();
        // Validate the access token and fetch the user ID
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(outgoingResponse, HttpStatus.BAD_REQUEST);
            return outgoingResponse;
        }
        // Validate the request body
        String errMsg = validateRequest(requestBody, outgoingResponse);
        if (StringUtils.isNotBlank(errMsg)) {
            return outgoingResponse;
        }
        String orgId = (String) requestBody.get(Constants.ORG_ID);
        // Generate the registration link
        String generateLink = serverProperties.getUrlCustomerSelfRegistration() /*+ orgId*/;
        qrBody.put(Constants.REGISTRATION_LINK, generateLink);
        String targetDirPath = String.format("%scustomregistration/%s/", Constants.LOCAL_BASE_PATH, orgId);
        String uniqueFileName = UUID.randomUUID() + ".png";
        String filePath = targetDirPath + uniqueFileName;
        File qrCodeFile;
        try {
            qrCodeBody = mapper.writeValueAsString(qrBody);
            pdfParams.put(Constants.SESSION, populateSession(qrCodeBody, filePath));
            qrCodeFile = pdfGeneratorService.generatePdfV2(pdfDetails, pdfParams);
        } catch (IOException e) {
            logger.error("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRAndLink :Error while parsing the QR code body", e);
            outgoingResponse.getParams().setStatus(HttpStatus.INTERNAL_SERVER_ERROR.toString());
            outgoingResponse.getParams().setErrmsg("Error while parsing the QR code object");
            outgoingResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return outgoingResponse;
        }
        // Upload the QR code image to storage
        outgoingResponse = storageService.uploadFile(qrCodeFile, serverProperties.getQrCustomerSelfRegistrationFolderName(), serverProperties.getQrCustomerSelfRegistrationContainerName());

        // Check if the upload was successful
        if (outgoingResponse.getResponseCode() == HttpStatus.OK) {
            logger.info("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRAndLink : QRCode Uploaded to bucket successfully.");
            String qrCodePath = String.format("%s/%s", serverProperties.getQrCustomerSelfRegistrationPath(), qrCodeFile.getName());
            Map<String, Object> data = updateOrgDetailsToDB(authUserToken, orgId, generateLink, qrCodePath);
            if (MapUtils.isEmpty(data) || !data.get(Constants.RESPONSE_CODE).equals(Constants.OK)) {
                logger.info("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRAndLink:Org data Updated successfully for the organization." + orgId);
                outgoingResponse.getParams().setStatus(Constants.FAILED);
                outgoingResponse.getParams().setErrmsg("Error while updating the organization details");
                outgoingResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return outgoingResponse;
            }
        } else {
            logger.info("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRAndLink : There was an issue while uploading the the qr code");
        }
        return outgoingResponse;
    }

    /**
     * Updates the organization details in the database.
     *
     * @param authUserToken The authentication token of the user making the request.
     * @param orgId         The ID of the organization to update.
     * @param generateLink  The registration link to update.
     * @param qrCodePath    The path of the qr code in the container.
     * @return A map containing the result of the update operation.
     */
    private Map<String, Object> updateOrgDetailsToDB(String authUserToken, String orgId, String generateLink, String qrCodePath) {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> updateRequest = new HashMap<>();
        Map<String, String> headerValues = new HashMap<>();
        headerValues.put(Constants.X_AUTH_TOKEN, authUserToken);
        request.put(Constants.ORGANIZATION_ID, orgId);
        request.put("registrationlink", generateLink);
        request.put("qrregistrationlink", qrCodePath);
        updateRequest.put(Constants.REQUEST, request);
        StringBuilder url = new StringBuilder(serverProperties.getSbUrl());
        url.append(serverProperties.getUpdateOrgPath());
        return outboundRequestHandlerService.fetchResultUsingPatch(
                String.valueOf(url), updateRequest, headerValues);
    }

    /**
     * Updates the error details in the API response.
     *
     * @param response     The API response object.
     * @param responseCode The HTTP status code.
     */
    private void updateErrorDetails(SBApiResponse response, HttpStatus responseCode) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
        response.setResponseCode(responseCode);
    }


    /**
     * Validates the request and updates the API response accordingly.
     *
     * @param request  The request object.
     * @param response The API response object.
     * @return An error message if the request is invalid, otherwise an empty string.
     */
    private String validateRequest(Map<String, Object> request, SBApiResponse response) {
        if (MapUtils.isEmpty(request)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("RequestBody is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Request Body is missing";
        } else if (StringUtils.isBlank((String) request.get(Constants.ORG_ID))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Organisation id is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Organisation id is missing";
        }
        return "";
    }

    /**
     * Populates the PDF template details with header, body, and footer information.
     *
     * @return A HashMap containing the PDF template details.
     */
    private HashMap<String, HashMap<String, String>> populatePDFTemplateDetails() {
        // Initialize an empty HashMap to store the PDF template details
        HashMap<String, HashMap<String, String>> pdfDetails = new HashMap<>();
        // Create a HashMap to store the header details
        HashMap<String, String> headerDetails = new HashMap<>();
        headerDetails.put(Constants.BUDGET_DOC_FILE_TYPE, Constants.VM);
        headerDetails.put(Constants.BUDGET_DOC_FILE_NAME, Constants.BATCH_SESSION_HEADER);
        // Log the header details for debugging purposes
        logger.info("CustomSelfRegistrationServiceImpl::populatePDFTemplateDetails : Header details: " + headerDetails);
        pdfDetails.put(Constants.HEADER, headerDetails);
        HashMap<String, String> bodyDetails = new HashMap<>();
        bodyDetails.put(Constants.BUDGET_DOC_FILE_TYPE, Constants.VM);
        bodyDetails.put(Constants.BUDGET_DOC_FILE_NAME, Constants.BATCH_SESSION_BODY_QR);
        pdfDetails.put(Constants.BODY, bodyDetails);
        logger.info("CustomSelfRegistrationServiceImpl::populatePDFTemplateDetails :Body details:" + bodyDetails);
        HashMap<String, String> footerDetails = new HashMap<>();
        footerDetails.put(Constants.BUDGET_DOC_FILE_TYPE, Constants.VM);
        footerDetails.put(Constants.BUDGET_DOC_FILE_NAME, Constants.BATCH_SESSION_FOOTER);
        logger.info("CustomSelfRegistrationServiceImpl::populatePDFTemplateDetails :Footer details: " + footerDetails);
        pdfDetails.put(Constants.FOOTER, footerDetails);
        logger.info("CustomSelfRegistrationServiceImpl::populatePDFTemplateDetails :PDF template details: " + pdfDetails);
        // Return the populated PDF template details
        return pdfDetails;
    }

    /**
     * Populates the PDF parameters with header and footer information.
     *
     * @return A HashMap containing the PDF parameters.
     */
    private HashMap<String, HashMap> populatePDFParams() {
        // Initialize an empty HashMap to store the PDF parameters
        HashMap<String, HashMap> params = new HashMap<>();
        // Create a HashMap to store the header parameters
        HashMap<String, String> headerParams = new HashMap<>();
        headerParams.put(Constants.PROGRAM_NAME, Constants.EMPTY);
        params.put(Constants.HEADER, headerParams);
        HashMap<String, String> footerParams = new HashMap<>();
        footerParams.put(Constants.PROGRAM_NAME, Constants.EMPTY);
        params.put(Constants.FOOTER, footerParams);
        logger.info("CustomSelfRegistrationServiceImpl::populatePDFParams : Footer parameters: " + footerParams);
        logger.info("CustomSelfRegistrationServiceImpl::populatePDFParams : PDF parameters:  " + params);
        return params;
    }

    /**
     * Populates a session HashMap with a QR code URL.
     *
     * @param qrCodeBody The body of the QR code.
     * @param filePath   The file path to generate the QR code.
     * @return A HashMap containing the session information.
     */
    private HashMap<String, String> populateSession(String qrCodeBody, String filePath) {
        // Initialize an empty HashMap to store the session information
        HashMap<String, String> session = new HashMap<>();
        session.put(Constants.QR_CODE_URL, generateCustomSelfRegistrationQRCode(qrCodeBody, filePath));
        return session;
    }

    /**
     * Generates a custom self-registration QR code and saves it to a file.
     *
     * @param qrCodeBody The body of the QR code.
     * @param filePath   The file path to save the QR code.
     * @return The absolute path of the generated QR code file.
     */
    public String generateCustomSelfRegistrationQRCode(String qrCodeBody, String filePath) {
        File qrCodeFile = QRCode.from(qrCodeBody).to(ImageType.PNG).file(filePath);
        logger.info("CustomSelfRegistrationServiceImpl::generateCustomSelfRegistrationQRCode : Generated QR code file path:" + qrCodeFile.getAbsolutePath());
        return qrCodeFile.getAbsolutePath();
    }
}
