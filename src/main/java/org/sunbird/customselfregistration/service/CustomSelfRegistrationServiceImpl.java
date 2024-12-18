package org.sunbird.customselfregistration.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.glxn.qrgen.core.image.ImageType;
import net.glxn.qrgen.javase.QRCode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.org.model.CustomeSelfRegistrationEntity;
import org.sunbird.customselfregistration.model.CustomSelfRegistrationModel;
import org.sunbird.org.repository.CustomSelfRegistrationRepository;
import org.sunbird.storage.service.StorageServiceImpl;
import org.sunbird.workallocation.service.PdfGeneratorServiceImpl;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


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

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    private CustomSelfRegistrationRepository qrRegistrationCodeRepository;

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

        // Validate the access token and fetch the user ID
        String userId = fetchUserIdFromToken(authUserToken, outgoingResponse);
        if (userId == null) return outgoingResponse;
        // Validate the request body
        String orgId = validateRequestBody(requestBody, outgoingResponse);
        if (orgId == null) return outgoingResponse;
        String errMsg = validateRequestFields(requestBody, outgoingResponse);
        if(!StringUtils.isEmpty(errMsg)) return outgoingResponse;
        isRegistrationQRCodeActive(orgId);
        //Validate the designation
        if (!isDesignationMappedToOrg(orgId, outgoingResponse)) return outgoingResponse;
        String uniqueId = String.valueOf(System.currentTimeMillis());
        String registrationLink = generateRegistrationLink(orgId,uniqueId);
        String qrCodeFilePath = createQRCodeFilePath(orgId);
        try {
            File qrCodeFile = generateQRCodeFile(registrationLink, qrCodeFilePath, orgId);
            File qrCodeLogoFile = QRCode.from(registrationLink).to(ImageType.JPG).withSize(750, 750).file(qrCodeFilePath);
            outgoingResponse = uploadQRCodeFile(qrCodeFile);
            SBApiResponse qrLogoUploadResponse = uploadQRCodeFile(qrCodeLogoFile);
            if (outgoingResponse.getResponseCode() == HttpStatus.OK && qrLogoUploadResponse.getResponseCode() == HttpStatus.OK) {
                CustomSelfRegistrationModel customSelfRegistrationModel = getCustomSelfRegistrationModel(requestBody, orgId, registrationLink, qrCodeFile, qrCodeLogoFile,userId, uniqueId);
                return processSuccessfulUpload(authUserToken, customSelfRegistrationModel, outgoingResponse);
            } else {
                logger.info("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRAndLink : There was an issue while uploading the QR code");
            }
        } catch (IOException e) {
            logger.error("CustomSelfRegistrationServiceImpl::getSelfRegistrationQRAndLink :Error while parsing the QR code body", e);
            outgoingResponse.getParams().setStatus(HttpStatus.INTERNAL_SERVER_ERROR.toString());
            outgoingResponse.getParams().setErrmsg("Error while parsing the QR code object");
            outgoingResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return outgoingResponse;
        }
        return outgoingResponse;
    }

    /**
     * Updates the organization details in the database.
     *
     * @param authUserToken The authentication token of the user making the request.
     * @param customSelfRegistrationModel the model containing custom self-registration details, including organization information.
     * @return A map containing the result of the update operation.
     */
    private Map<String, Object> updateOrgDetailsToDB(String authUserToken, CustomSelfRegistrationModel customSelfRegistrationModel) {
        logger.info("CustomSelfRegistrationServiceImpl::updateOrgDetailsToDB:Updating the Org details for the organization." + customSelfRegistrationModel.getOrgId());
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> updateRequest = new HashMap<>();
        Map<String, String> headerValues = new HashMap<>();
        headerValues.put(Constants.X_AUTH_TOKEN, authUserToken);
        request.put(Constants.ORGANIZATION_ID, customSelfRegistrationModel.getOrgId());
        request.put(Constants.REGISTRATION_LINK_CSR, customSelfRegistrationModel.getRegistrationLink());
        request.put(Constants.QR_REGISTRATION_LINK_CSR, customSelfRegistrationModel.getQrCodeFilePath());
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
     * @param orgId Id of the organisation
     * @return A HashMap containing the session information.
     */
    private HashMap<String, String> populateSession(String qrCodeBody, String filePath, String orgId) {
        // Initialize an empty HashMap to store the session information
        HashMap<String, String> session = new HashMap<>();
        session.put(Constants.QR_CODE_URL, generateCustomSelfRegistrationQRCode(qrCodeBody, filePath));
        session.put(Constants.ORGANIZATION_ID, orgId);
        session.put(Constants.REGISTRATION_LINK_CSR, qrCodeBody);
        List<Map<String, Object>> cassandraResponse = fetchOrgDetailsById(orgId);
        String orgName= (String) cassandraResponse.get(0).get(Constants.ORG_NAME_LOWERCASE);
        session.put(Constants.ORGANISATION_NAME,orgName);

        logger.info("CustomSelfRegistrationServiceImpl::populateSession : Session information: " + session.get(Constants.ORGANIZATION_ID) +" "+ session.get(Constants.ORGANISATION_NAME));
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
        File qrCodeFile = QRCode.from(qrCodeBody).to(ImageType.JPG).withSize(750,750).file(filePath);
        logger.info("CustomSelfRegistrationServiceImpl::generateCustomSelfRegistrationQRCode : Generated QR code file path:" + qrCodeFile.getAbsolutePath());
        return qrCodeFile.getAbsolutePath();
    }


    /**
     * Validates the designation for the given organization.
     *
     * @param orgId The ID of the organization.
     * @return True if the designation is valid, false otherwise.
     */
    private boolean validateDesignation(String orgId) {
        logger.info("CustomSelfRegistrationServiceImpl::validateDesignation.. started");
        Map<String, Object> properyMap = new HashMap<>();
        try {
            properyMap.put(Constants.ID, orgId);
            List<Map<String, Object>> cassandraResponse = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD,
                    Constants.TABLE_ORGANIZATION, properyMap, null);

            // Get the framework ID from the cassandra response
            String frameworkID = (String) cassandraResponse.get(0).get(Constants.FRAMEWORK_ID_KEY);
            if (StringUtil.isBlank(frameworkID)) {
                return false;
            }
            // Construct the framework read URL
            String url = serverProperties.getKmBaseHost() + serverProperties.getFrameworkReadEndpoint() + Constants.SLASH + frameworkID;
            logger.info("CustomSelfRegistrationServiceImpl::validateDesignation:framework read url:: " + url);

            Map<String, Object> frameworkReadResponse = (Map<String, Object>) outboundRequestHandlerService.fetchResult(url);

            // Check if the framework read response is null or does not contain the result key
            if (frameworkReadResponse == null || !frameworkReadResponse.containsKey(Constants.RESULT)) {
                logger.info("CustomSelfRegistrationServiceImpl::validateDesignation:Failed to read framework");
                return false;
            }

            Map<String, Object> frameworkResponseList = (Map<String, Object>) frameworkReadResponse.get(Constants.RESULT);
            Map<String, Object> frameworkMap = (Map<String, Object>) frameworkResponseList.get(Constants.FRAMEWORK);
            List<Map<String, Object>> categories = (List<Map<String, Object>>) frameworkMap.get(Constants.CATEGORIES);

            // Check if the categories list is empty
            if (CollectionUtils.isEmpty(categories)) {
                logger.info("CustomSelfRegistrationServiceImpl::validateDesignation:no categories found in read framework");
                return false;
            }

            // Validate the designation association
            return isDesignationAssosciationValid(categories);
        } catch (Exception e) {
            logger.info("CustomSelfRegistrationServiceImpl::validateDesignation:Failed validate designation for orgId: " + orgId, e);
            return false;
        }
    }

    /**
     * Validates the designation association for the given categories.
     *
     * @param categories The list of categories to validate.
     * @return True if the designation association is valid, false otherwise.
     */
    private  boolean isDesignationAssosciationValid(List<Map<String, Object>> categories) {
        logger.info("CustomSelfRegistrationServiceImpl::isDesignationAssosciationValid.. started");
        // Iterate through each category
        for (Map<String, Object> category : categories) {
            // Get the category code
            String categoryCode = (String) category.get(Constants.CODE);
            // Check if the category code is ORG
            if (Constants.ORG.equalsIgnoreCase(categoryCode)) {
                // Get the terms for the category
                List<Map<String, Object>> terms = (List<Map<String, Object>>) category.get(Constants.TERMS);
                // Check if the terms list is not empty
                if (CollectionUtils.isNotEmpty(terms)) {
                    // Get the designation association from the terms
                    Map<String, Object> designationAssociation = terms.get(0);
                    // Check if the designation association is not empty and contains associations
                    if (MapUtils.isNotEmpty(designationAssociation) &&
                            designationAssociation.containsKey(Constants.ASSOCIATIONS) &&
                            !CollectionUtils.isEmpty((Collection) designationAssociation.get(Constants.ASSOCIATIONS))) {
                        logger.info("CustomSelfRegistrationServiceImpl::isDesignationAssosciationValid.. Designation Assosciation is valid");
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Fetches the user ID from the provided authentication token.
     *
     * @param authUserToken the authentication token to extract the user ID from
     * @param response      the API response object to update with error details if necessary
     * @return the user ID extracted from the token, or null if the token is invalid
     */
    private String fetchUserIdFromToken(String authUserToken, SBApiResponse response) {
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
        if (ObjectUtils.isEmpty(userId)) {
            updateErrorDetails(response, HttpStatus.BAD_REQUEST);
        }
        return userId;
    }

    /**
     * Validates the request body and extracts the organization ID if valid.
     *
     * @param requestBody the request body to validate
     * @param response    the API response object to update with error details if necessary
     * @return the organization ID if the request body is valid, or null if validation fails
     */
    private String validateRequestBody(Map<String, Object> requestBody, SBApiResponse response) {
        String errMsg = validateRequest(requestBody, response);
        if (StringUtils.isNotBlank(errMsg)) return null;
        return (String) requestBody.get(Constants.ORG_ID);
    }

    /**
     * Checks if a designation is mapped to a specific organization.
     *
     * @param orgId    the organization ID to check
     * @param response the API response object to update with error details if necessary
     * @return true if the designation is mapped to the organization, false otherwise
     */
    private boolean isDesignationMappedToOrg(String orgId, SBApiResponse response) {
        if (!validateDesignation(orgId)) {
            logger.info("CustomSelfRegistrationServiceImpl::isDesignationMappedToOrg:Designation is not mapped to the organization: " + orgId);
            response.getParams().setStatus(HttpStatus.OK.toString());
            response.getParams().setErrmsg("Designation is not mapped to the organization");
            response.setResponseCode(HttpStatus.OK);
            return false;
        }
        return true;
    }

    /**
     * Generates a registration link for a given organization ID.
     *
     * @param orgId the organization ID to generate the registration link for
     * @return the generated registration link
     */
    private String generateRegistrationLink(String orgId, String id) {
        return serverProperties.getUrlCustomerSelfRegistration() + "/crp/" + id + "/" + orgId;
    }

    /**
     * Creates a file path for a QR code image for a given organization ID.
     *
     * @param orgId the organization ID to create the QR code file path for
     * @return the generated file path for the QR code image
     */
    private String createQRCodeFilePath(String orgId) {
        String targetDirPath = String.format("%scustomregistration/%s/", Constants.LOCAL_BASE_PATH, orgId);
        return targetDirPath + UUID.randomUUID() + ".png";
    }

    /**
     * Generates a QR code file for a given registration link and file path.
     *
     * @param registrationLink the registration link to encode in the QR code
     * @param filePath         the file path to save the generated QR code file to
     * @param orgId Id of the organisation
     * @return the generated QR code file
     * @throws IOException if an error occurs during QR code generation
     */
    private File generateQRCodeFile(String registrationLink, String filePath, String orgId) throws IOException {
        String qrCodeBody = registrationLink;
        HashMap<String, HashMap> pdfParams = populatePDFParams();
        pdfParams.put(Constants.SESSION, populateSession(qrCodeBody, filePath,orgId));
        HashMap<String, HashMap<String, String>> pdfDetails = populatePDFTemplateDetails();
        return pdfGeneratorService.generatePdfV2(pdfDetails, pdfParams);
    }

    /**
     * Uploads a QR code file to the storage service.
     *
     * @param qrCodeFile the QR code file to upload
     * @return the API response from the storage service
     */
    private SBApiResponse uploadQRCodeFile(File qrCodeFile) {
        return storageService.uploadFile(
                qrCodeFile,
                serverProperties.getQrCustomerSelfRegistrationFolderName(),
                serverProperties.getQrCustomerSelfRegistrationContainerName()
        );
    }


    /**
     * Processes a successful upload of a QR code file.
     *
     * @param authUserToken    the authentication token for the user
     * @param customSelfRegistrationModel the model containing custom self-registration details, including organization information.
     * @param response         the API response object
     * @return the updated API response object
     */
    private SBApiResponse processSuccessfulUpload(String authUserToken, CustomSelfRegistrationModel customSelfRegistrationModel, SBApiResponse response) {
        Map<String, Object> data = updateOrgDetailsToDB(authUserToken, customSelfRegistrationModel);
        updateQRCodeDetailsToDB(customSelfRegistrationModel);
        if (MapUtils.isEmpty(data) || !data.get(Constants.RESPONSE_CODE).equals(Constants.OK)) {
            logger.info("CustomSelfRegistrationServiceImpl::processSuccessfulUpload:Failed to update Org details for organization: " + customSelfRegistrationModel.getOrgId());
            setInternalServerError(response, "Error while updating the organization details");
        }else {
            response = new SBApiResponse();
            populateSuccessResponse(response,customSelfRegistrationModel);
        }

        return response;
    }

    /**
     * Sets the internal server error response for a given API response object.
     *
     * @param response the API response object to update
     * @param errorMsg the error message to include in the response
     */
    private void setInternalServerError(SBApiResponse response, String errorMsg) {
        response.getParams().setStatus(HttpStatus.INTERNAL_SERVER_ERROR.toString());
        response.getParams().setErrmsg(errorMsg);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * Populates a successful API response object with registration link and QR code path.
     *
     * @param response         the API response object to update
     @param customSelfRegistrationModel the model containing custom self-registration details, including organization information.
     */
    private void populateSuccessResponse(SBApiResponse response, CustomSelfRegistrationModel customSelfRegistrationModel) {
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.REGISTRATION_LINK_CSR, customSelfRegistrationModel.getRegistrationLink());
        result.put(Constants.QR_REGISTRATION_LINK_CSR, customSelfRegistrationModel.getQrCodeFilePath());
        result.put(Constants.QR_LOGO_PATH, customSelfRegistrationModel.getQrLogoFilePath());
        response.getResult().putAll(result);
        response.getParams().setStatus(Constants.OK);
        response.setResponseCode(HttpStatus.OK);
    }

    /**
     * Validates specific fields in the request and updates the API response accordingly.
     *
     * @param request  The request object.
     * @param response The API response object.
     * @return An error message if any required field is invalid, otherwise an empty string.
     */
    private String validateRequestFields(Map<String, Object> request, SBApiResponse response) {
        if (request.get(Constants.REGISTRATION_END_DATE) == null || (Long) request.get(Constants.REGISTRATION_END_DATE) <= 0) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Registration end date is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Registration end date is missing";
        } else if (request.get(Constants.REGISTRATION_START_DATE) == null || (Long) request.get(Constants.REGISTRATION_START_DATE) <= 0) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Registration start date is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Registration start date is missing";
        }
        return "";
    }

    /**
     * Creates a CustomSelfRegistrationModel instance based on the provided request body and parameters.
     *
     * @param orgId         The organization ID.
     * @param registrationLink The registration link.
     * @param qrCodeFile    The QR code file.
     * @param qrCodeLogoFile    The QR code logo file.
     * @param userId        The user ID.
     * @param uniqueId  currenttimestamp as string is appended in the url path.
     * @return A CustomSelfRegistrationModel instance.
     */
    public CustomSelfRegistrationModel getCustomSelfRegistrationModel(Map<String, Object> requestBody, String orgId, String registrationLink, File qrCodeFile,File qrCodeLogoFile, String userId, String uniqueId) {
        logger.info("CustomSelfRegistrationServiceImpl::getCustomSelfRegistrationModel : Creating the CustomSelfRegistrationModel instance for organization: " + orgId);
        ZoneId zoneId = ZoneId.of("Asia/Kolkata");
        ZonedDateTime registrationStartDateLong = Instant.ofEpochMilli(Long.parseLong(String.valueOf(requestBody.get(Constants.REGISTRATION_START_DATE)))).atZone(zoneId);
        ZonedDateTime registrationEndDateLong = Instant.ofEpochMilli(Long.parseLong(String.valueOf(requestBody.get(Constants.REGISTRATION_END_DATE)))).atZone(zoneId)
                .withHour(23)
                .withMinute(59)
                .withSecond(59);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        String formattedRegistrationStartDate = registrationStartDateLong.format(formatter);
        String formattedRegistrationEndDate = registrationEndDateLong.format(formatter);
        return CustomSelfRegistrationModel.builder()
                .orgId(orgId)
                .registrationLink(registrationLink)
                .qrCodeFilePath(String.format("%s/%s", serverProperties.getQrCustomerSelfRegistrationPath(), qrCodeFile.getName()))
                .qrLogoFilePath(String.format("%s/%s", serverProperties.getQrCustomerSelfRegistrationPath(), qrCodeLogoFile.getName()))
                .status(Constants.ACTIVE)
                .createdby(userId)
                .numberofusersonboarded(0L)
                .id(uniqueId)
                .registrationenddate(formattedRegistrationEndDate)
                .registrationstartdate(formattedRegistrationStartDate)
                .createddatetime(ZonedDateTime.now(zoneId).format(formatter))
                .build();
    }

    /**
     * Checks if the registration QR code is active for the given organization ID.
     *
     * @param orgId The organization ID.
     *
     */
    private void isRegistrationQRCodeActive(String orgId) {
        logger.info("CustomSelfRegistrationServiceImpl::isRegistrationQRCodeActive : Checking if registration QR code is active for orgId: {}", orgId);
        List<CustomeSelfRegistrationEntity> qrRegistrationCodeByOrgIds = qrRegistrationCodeRepository.findAllByOrgId(orgId);
        Optional.ofNullable(qrRegistrationCodeByOrgIds).orElse(Collections.emptyList()).
                stream().
                filter(qrCodeDetails -> Constants.ACTIVE.equalsIgnoreCase(qrCodeDetails.getStatus())).
                forEach(qrCodeDetails -> {
                    try {
                        qrRegistrationCodeRepository.updateRegistrationQrCodeWithStatus(qrCodeDetails.getOrgId(), qrCodeDetails.getId(), "expired");
                    } catch (Exception e) {
                        logger.error("CustomSelfRegistrationServiceImpl::getCustomSelfRegistrationModel :Failed to update QR code with ID {}: {}", qrCodeDetails.getId(), e.getMessage());
                    }
                });
    }

    /**
     * Updates the QR code details in the database using the provided CustomSelfRegistrationModel.
     *
     * @param customSelfRegistrationModel The CustomSelfRegistrationModel containing the updated details.
     */
    private void updateQRCodeDetailsToDB(CustomSelfRegistrationModel customSelfRegistrationModel) {
        logger.info("CustomSelfRegistrationServiceImpl::updateQRCodeDetailsToDB : Inserting QR code details for orgId: {}", customSelfRegistrationModel.getOrgId());
        CustomeSelfRegistrationEntity registrationQRCode = CustomeSelfRegistrationEntity.builder()
                .orgId(customSelfRegistrationModel.getOrgId())
                .id(customSelfRegistrationModel.getId())
                .status(customSelfRegistrationModel.getStatus())
                .url(customSelfRegistrationModel.getRegistrationLink())
                .startDate(customSelfRegistrationModel.getRegistrationstartdate())
                .endDate(customSelfRegistrationModel.getRegistrationenddate())
                .createdBy(customSelfRegistrationModel.getCreatedby())
                .createdDateTime(customSelfRegistrationModel.getCreateddatetime())
                .numberOfUsersOnboarded(customSelfRegistrationModel.getNumberofusersonboarded())
                .qrCodeImagePath(customSelfRegistrationModel.getQrCodeFilePath())
                .qrCodeLogoPath(customSelfRegistrationModel.getQrLogoFilePath())
                .build();
        qrRegistrationCodeRepository.save(registrationQRCode);
    }


    /**
     * Retrieves the list of all registration done through QR codes for a given organization.
     *
     * @param authUserToken The authentication token of the user making the request.
     * @param requestBody   The request body containing the organization ID.
     * @return SBApiResponse containing the list of registration done through QR codes or an error message if the request fails.
     */
    @Override
    public SBApiResponse getAllRegistrationQRCodesList(String authUserToken, Map<String, Object> requestBody) {
        logger.info("CustomSelfRegistrationServiceImpl::getAllRegistrationQRCodesList : Retrieving all the registration QR codes done through qr for orgId");
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CUSTOM_SELF_REGISTRATION_CREATE_API);
        // Validate the access token and fetch the user ID
        String userId = fetchUserIdFromToken(authUserToken, outgoingResponse);
        if (StringUtils.isBlank(userId)) return outgoingResponse;
        // Validate the request body
        String orgId = validateRequestBody(requestBody, outgoingResponse);
        if (StringUtils.isBlank(orgId)) return outgoingResponse;
        // Get the QR code details
        List<CustomeSelfRegistrationEntity> qrRegistrationCodeByOrgIds = qrRegistrationCodeRepository.findAllByOrgId(orgId);
        if (CollectionUtils.isEmpty(qrRegistrationCodeByOrgIds)) {
            outgoingResponse.getParams().setStatus(Constants.FAILED);
            outgoingResponse.getParams().setErrmsg("No QR Code found for this org");
            outgoingResponse.setResponseCode(HttpStatus.OK);
            return outgoingResponse;
        } else {
            Map<String, Object> result = new HashMap<>();
            result.put("qrCodeDataForOrg", qrRegistrationCodeByOrgIds);
            outgoingResponse.getResult().putAll(result);
            outgoingResponse.getParams().setStatus(Constants.OK);
            outgoingResponse.setResponseCode(HttpStatus.OK);
        }
        return outgoingResponse;
    }

    /**
     * Expire registration QR codes for a given organization ID.
     *
     * @param requestBody The request body containing the organization ID.
     * @return The API response with the updated organization IDs and QR code IDs.
     */
    @Override
    public SBApiResponse expireRegistrationQRCodes(Map<String, Object> requestBody) {
        logger.info("CustomSelfRegistrationServiceImpl::expireRegistrationQRCodes");
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CUSTOM_SELF_REGISTRATION_CREATE_API);
        // Validate the request body
        String orgId = validateRequestBody(requestBody, outgoingResponse);
        if (StringUtils.isBlank(orgId)) return outgoingResponse;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        ZoneId zoneId = ZoneId.of("Asia/Kolkata");
        LocalDateTime currentDate = LocalDateTime.now(zoneId);
        List<CustomeSelfRegistrationEntity> qrRegistrationCodeByOrgIds = qrRegistrationCodeRepository.findAllByOrgId(orgId);
        List<String> orgIds = new ArrayList<>();
        for (CustomeSelfRegistrationEntity record : qrRegistrationCodeByOrgIds) {
            String status = record.getStatus();
            String endDateStr = record.getEndDate();
            if (Constants.ACTIVE.equalsIgnoreCase(status) && !StringUtils.isEmpty(endDateStr)) {
                try {
                    LocalDateTime endDate = LocalDateTime.parse(endDateStr, formatter);
                    if (currentDate.isAfter(endDate)) {
                        qrRegistrationCodeRepository.updateRegistrationQrCodeWithStatus(record.getOrgId(), record.getId(), "expired");
                        orgIds.add(record.getOrgId() + "-" + record.getId());
                        logger.info("CustomSelfRegistrationServiceImpl::expireRegistrationQRCodes : Updated the data for the orgId " + orgId + " and id " + record.getId());
                    }
                } catch (Exception e) {
                    logger.error("Error while updating the data for the orgId " + orgId + " and id " + record.getId());
                }
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put("orgIdsUniqueIdsUpdated", orgIds);
        outgoingResponse.getResult().putAll(result);
        outgoingResponse.getParams().setStatus(Constants.OK);
        outgoingResponse.setResponseCode(HttpStatus.OK);
        return outgoingResponse;
    }

    /**
     * Uploads an image to a Google Cloud Platform (GCP) container.
     *
     * @param multipartFile the image file to be uploaded
     * @param authUserToken the authentication token for the user
     * @return the response object containing the result of the upload operation
     */
    @Override
    public SBApiResponse uploadImageToGCPContainer(MultipartFile multipartFile, String authUserToken) {
        logger.info("CustomSelfRegistrationServiceImpl::uploadImageToGCPContainer:started");
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.CLOUD_FOLDER_NAME, serverProperties.getQrCustomerSelfRegistrationLogoFolderName());
        logger.debug("Added cloud folder name to request body: {}", Constants.CLOUD_FOLDER_NAME);
        requestBody.put(Constants.CLOUD_CONTAINER_NAME, serverProperties.getQrCustomerSelfRegistrationContainerName());
        logger.debug("Added cloud container name to request body: {}", Constants.CLOUD_CONTAINER_NAME);
        SBApiResponse response = storageService.uploadImageToGCPContainer(multipartFile, requestBody, authUserToken);
        logger.info("Received response from storage service: {}", response);
        String fileName = response.getResult().get(Constants.NAME).toString();
        String filePath = serverProperties.getQrCustomerSelfRegistrationLogoPath() + fileName;
        response.getResult().put(Constants.QR_CODE_PATH, filePath);
        return response;
    }

    /**
     * Checks if the registration QR code is active for the given request.
     *
     * @param requestBody A map containing the request body parameters.
     * @return SBApiResponse containing the result of the request or an error message if the request fails.
     */
    @Override
    public SBApiResponse isRegistrationQRActive(Map<String, Object> requestBody) {
        logger.info("CustomSelfRegistrationServiceImpl::isRegistrationQRActive  Started ");
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.IS_REGISTRATION_LINK_ACTIVE);
        String errMsg = validateRequestBodyForQRCodeActive(requestBody, outgoingResponse);
        if (StringUtils.isNotEmpty(errMsg)) return outgoingResponse;
        String registrationLink = (String) requestBody.get("registrationLink");
        String[] ids = extractIdsFromUrl(registrationLink);
        String orgId = null;
        String uniqueCode = null;
        if (StringUtils.isNotEmpty(ids[0]) && StringUtils.isNotEmpty(ids[1])) {
            uniqueCode = ids[0];
            orgId = ids[1];
        }
        String errMsgForRegistration = validateRegistrationLink(orgId, uniqueCode,outgoingResponse);
        if (StringUtils.isNotEmpty(errMsgForRegistration)) return outgoingResponse;
        logger.info("CustomSelfRegistrationServiceImpl::isRegistrationQRActive :" + orgId + " " + uniqueCode);
        CustomeSelfRegistrationEntity customeSelfRegistrationEntity = qrRegistrationCodeRepository.findAllByOrgIdAndUniqueId(orgId, uniqueCode);
        if (Objects.isNull(customeSelfRegistrationEntity)) {
            logger.info("CustomSelfRegistrationServiceImpl::isRegistrationQRActive : No registration data found for orgId " + orgId + " and uniqueCode " + uniqueCode);
            outgoingResponse.getParams().setStatus(Constants.FAILED);
            outgoingResponse.getParams().setErrmsg("Registration link is missing");
            outgoingResponse.setResponseCode(HttpStatus.BAD_REQUEST);
            return outgoingResponse;
        }
        String startDate = customeSelfRegistrationEntity.getStartDate();
        String endDate = customeSelfRegistrationEntity.getEndDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        ZonedDateTime registrationStartDate = ZonedDateTime.parse(startDate, formatter.withZone(ZoneId.of(Constants.ASIA_CALCUTTA_TIMEZONE)));
        ZonedDateTime registrationEndDate = ZonedDateTime.parse(endDate, formatter.withZone(ZoneId.of(Constants.ASIA_CALCUTTA_TIMEZONE)));
        ZonedDateTime currentDateTime = ZonedDateTime.now(ZoneId.of(Constants.ASIA_CALCUTTA_TIMEZONE));
        if (currentDateTime.isAfter(registrationStartDate) && currentDateTime.isBefore(registrationEndDate) && customeSelfRegistrationEntity.getStatus().equals(Constants.ACTIVE)) {
            logger.info("CustomSelfRegistrationServiceImpl::isRegistrationQRActive : Registration link is active for orgId " + orgId + " and uniqueCode " + uniqueCode);
            outgoingResponse.getParams().setStatus(Constants.OK);
            outgoingResponse.getParams().setErrmsg("Registration link is active");
            outgoingResponse.setResponseCode(HttpStatus.OK);
        } else {
            outgoingResponse.getParams().setStatus(Constants.FAILED);
            outgoingResponse.getParams().setErrmsg("Registration link is not active");
            outgoingResponse.setResponseCode(HttpStatus.BAD_REQUEST);
        }
        return outgoingResponse;
    }
    /**
     * Validates the request body for the isRegistrationQRActive method.
     *
     * @param request  The request body map.
     * @param response The response object to be updated with error messages.
     * @return An error message if the validation fails, otherwise an empty string.
     */
    private String validateRequestBodyForQRCodeActive(Map<String, Object> request, SBApiResponse response) {
        if (MapUtils.isEmpty(request)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("RequestBody is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Request Body is missing";
        } else if (StringUtils.isBlank((String) request.get("registrationLink"))) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Registration link is missing");
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Registration link is missing";
        }
        return "";
    }
    /**
     * Extracts orgid and unique code from the URL.
     *
     * @param url The URL to extract the IDs from.
     * @return An array of two strings containing the extracted IDs, or null if no match is found.
     */
    public static String[] extractIdsFromUrl(String url) {
        // Regex pattern to capture two numeric IDs after "/crp/"
        String regex = "/crp/(\\d+)/(\\d+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(url);
        // If the pattern matches, return the captured IDs
        if (matcher.find()) {
            return new String[]{matcher.group(1), matcher.group(2)}; // Group 1 and Group 2 are the IDs
        }
        // Return an empty array instead of null to avoid NullPointerException
        return new String[]{"", ""};
    }

    /**
     * Expire registration QR codes for a given organization ID.
     *
     * @return The API response with the updated organization IDs and QR code IDs.
     */
    @Override
    public SBApiResponse expireRegistrationQRCodesByCronJob() {
        logger.info("CustomSelfRegistrationServiceImpl::expireRegistrationQRCodes");
        SBApiResponse outgoingResponse = ProjectUtil.createDefaultResponse(Constants.CUSTOM_SELF_REGISTRATION_CREATE_API);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        ZoneId zoneId = ZoneId.of("Asia/Kolkata");
        LocalDateTime currentDate = LocalDateTime.now(zoneId);
        List<CustomeSelfRegistrationEntity> qrRegistrationCodeByOrgIds = qrRegistrationCodeRepository.findAll();
        List<String> orgIds = new ArrayList<>();
        for (CustomeSelfRegistrationEntity record : qrRegistrationCodeByOrgIds) {
            String status = record.getStatus();
            String endDateStr = record.getEndDate();
            String orgId = record.getOrgId();
            if (Constants.ACTIVE.equalsIgnoreCase(status) && !StringUtils.isEmpty(endDateStr)) {
                try {
                    LocalDateTime endDate = LocalDateTime.parse(endDateStr, formatter);
                    if (currentDate.isAfter(endDate)) {
                        qrRegistrationCodeRepository.updateRegistrationQrCodeWithStatus(record.getOrgId(), record.getId(), "expired");
                        orgIds.add(record.getOrgId() + "-" + record.getId());
                        logger.info("CustomSelfRegistrationServiceImpl::expireRegistrationQRCodes : Updated the data for the orgId " + orgId + " and id " + record.getId());
                    }
                } catch (Exception e) {
                    logger.error("Error while updating the data for the orgId " + orgId + " and id " + record.getId());
                }
            }
        }
        Map<String, Object> result = new HashMap<>();
        result.put("orgIdsUniqueIdsUpdated", orgIds);
        outgoingResponse.getResult().putAll(result);
        outgoingResponse.getParams().setStatus(Constants.OK);
        outgoingResponse.setResponseCode(HttpStatus.OK);
        return outgoingResponse;
    }

    /**
     * Validates a registration link by checking the organization ID and unique code.
     *
     * @param orgId         the organization ID
     * @param uniqueCode    the unique code
     * @param outgoingResponse the response object to be populated with the validation result
     * @return an error message if the validation fails, or an empty string if it succeeds
     */
    private String validateRegistrationLink(String orgId, String uniqueCode, SBApiResponse outgoingResponse) {
        logger.info("CustomSelfRegistrationServiceImpl::validateRegistrationLink" + orgId + " " + uniqueCode);
        List<Map<String, Object>> orgDetails = fetchOrgDetailsById(orgId);
        if (Objects.isNull(orgDetails) || orgDetails.isEmpty()) {
            logger.info("CustomSelfRegistrationServiceImpl::validateRegistrationLink : No data found for orgId :" + orgId);
            outgoingResponse.getParams().setStatus(Constants.FAILED);
            outgoingResponse.getParams().setErrmsg("Invalid Organisation is Provided in the Registration Link");
            outgoingResponse.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Invalid Organisation is Provided in the Registration Link";
        }
        CustomeSelfRegistrationEntity customeSelfRegistrationEntity = qrRegistrationCodeRepository.findAllByUniqueId(uniqueCode);
        if (Objects.isNull(customeSelfRegistrationEntity)) {
            logger.info("CustomSelfRegistrationServiceImpl::validateRegistrationLink : No data found for uniqueCode :" + uniqueCode);
            outgoingResponse.getParams().setStatus(Constants.FAILED);
            outgoingResponse.getParams().setErrmsg("Invalid Unique Code is Provided in the Registration Link");
            outgoingResponse.setResponseCode(HttpStatus.BAD_REQUEST);
            return "Invalid Unique Code is Provided in the Registration Link";
        }
        return "";
    }

    /**
     * Fetches organization details by ID from the Cassandra database.
     *
     * @param orgId the organization ID
     * @return a list of maps containing the organization details
     */
    private List<Map<String, Object>> fetchOrgDetailsById(String orgId) {
        Map<String, Object> properyMap = new HashMap<>();
        properyMap.put(Constants.ID, orgId);
        return cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_ORGANIZATION, properyMap, null);
    }

}
