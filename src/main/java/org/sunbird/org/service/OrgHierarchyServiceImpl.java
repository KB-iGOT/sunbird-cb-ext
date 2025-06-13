package org.sunbird.org.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.org.util.ExcelUtil;
import org.sunbird.org.util.FrameworkUtil;
import org.sunbird.storage.service.StorageServiceImpl;
import org.sunbird.user.service.UserUtilityService;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.*;

@Service
public class OrgHierarchyServiceImpl implements OrgHierarchyService {

    private final Logger logger = LoggerFactory.getLogger(OrgHierarchyServiceImpl.class);


    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    RedisCacheMgr redisCacheMgr;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    ExcelUtil excelUtil;

    @Autowired
    FrameworkUtil frameworkUtil;

    @Autowired AccessTokenValidator accessTokenValidator;

    @Autowired
    StorageServiceImpl storageService;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    private UserUtilityService userUtilityService;


    @Override
    public ResponseEntity<ByteArrayResource> bulkUploadOrganisationMapping(String rootOrgId, String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            Sheet referenceSheetCompetency = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyReferenceWorkSpaceName()));
            Sheet orgDesignationMasterSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyMasterDesignationWorkSpaceName()));
            List<Map<String, Object>> categoryList = frameworkUtil.populateDataFromFrameworkTerm(frameworkId);
            String[] referenceSheetHeaders = categoryList.stream()
                    .map(cat -> (String) cat.get(Constants.CODE))
                    .filter(code -> code != null)
                    .toArray(String[]::new);
            excelUtil.createHeaderRowForYourWorkBook(referenceSheetCompetency, referenceSheetHeaders);
            excelUtil.createHeaderRow(orgDesignationMasterSheet, serverProperties.getBulkUploadOrgHierarchyMasterDataHeaders());

            String orgId = frameworkId.split("_")[0];
            frameworkUtil.populateOrgDesignationMaster(orgDesignationMasterSheet, orgId);

            excelUtil.makeSheetReadOnly(orgDesignationMasterSheet);

            orgDesignationMasterSheet.setColumnHidden(2, true);

            excelUtil.setUpDropdowns(workbook, referenceSheetCompetency, orgDesignationMasterSheet, serverProperties.getBulkUploadOrgHierarchyReferencesHeaders());

            excelUtil.setColumnWidths(referenceSheetCompetency);

            excelUtil.setColumnWidths(orgDesignationMasterSheet);

            excelUtil.addDuplicateHighlighting(referenceSheetCompetency, 1, 1000, 0, serverProperties.getBulkUploadOrgHierarchyReferencesHeaders().length - 1);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);

            workbook.close();

            // Convert the output stream to a byte array and return as a downloadable file
            ByteArrayResource resource = new ByteArrayResource(outputStream.toByteArray());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverProperties.getOrgHierarchyBulkUploadFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            logger.error("Error while generating bulk upload organisation mapping file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    @Override
    public ResponseEntity<ByteArrayResource> exportOrgHierarchyToExcel(String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            Sheet referenceSheetCompetency = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyReferenceWorkSpaceName()));
            List<Map<String, Object>> categoryList = frameworkUtil.populateDataFromFrameworkTerm(frameworkId);
            String[] referenceSheetHeaders = categoryList.stream()
                    .map(cat -> (String) cat.get(Constants.CODE))
                    .filter(code -> code != null)
                    .toArray(String[]::new);
            excelUtil.createHeaderRowForYourWorkBook(referenceSheetCompetency, referenceSheetHeaders);

            excelUtil.makeSheetReadOnly(referenceSheetCompetency);


            excelUtil.setColumnWidths(referenceSheetCompetency);

            excelUtil.populateReferenceSheetCompetency(categoryList, referenceSheetCompetency);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);

            workbook.close();

            // Convert the output stream to a byte array and return as a downloadable file
            ByteArrayResource resource = new ByteArrayResource(outputStream.toByteArray());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverProperties.getOrgHierarchyBulkUploadFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            logger.error("Error while exporting organisation hierarchy to Excel", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }


    @Override
    public SBApiResponse bulkUploadOrgHierarchyMapping(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMPETENCY_DESIGNATION_EVENT_BULK_UPLOAD);
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            if (!ProjectUtil.hasValidRowCountInXLSFile(file, serverProperties.getMaximumRowAllowedForDesignationCompetencyUpload())) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(MessageFormat.format(
                        Constants.BULK_UPLOAD_MAXIMUM_LIMIT_ERROR_MSG,
                        serverProperties.getMaximumRowAllowedForDesignationCompetencyUpload()
                ));
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getOrgHierarchyBulkUploadContainerName());
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                setErrorData(response, String.format("Failed to upload file. Error: %s",
                        (String) uploadResponse.getParams().getErrmsg()), HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            Map<String, Object> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.ROOT_ORG_ID, rootOrgId);
            uploadedFile.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
            uploadedFile.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
            uploadedFile.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
            uploadedFile.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
            uploadedFile.put(Constants.STATUS, Constants.INITIATED_CAPITAL);
            uploadedFile.put(Constants.CREATED_BY, userId);

            SBApiResponse insertResponse = cassandraOperation.insertRecord(Constants.DATABASE,
                    Constants.ORG_HIERARCHY_MAPPING_BULK_UPLOAD, uploadedFile);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with org competency Designation bulk details.", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(uploadedFile);
            uploadedFile.put(Constants.X_AUTH_TOKEN, userAuthToken);
            uploadedFile.put(Constants.FRAMEWORK_ID, frameworkId);
            kafkaProducer.pushWithKey(serverProperties.getOrgHierarchyBulkUploadTopic(), uploadedFile, rootOrgId);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to process Org competency Designation bulk upload request. Error: ", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private void setErrorData(SBApiResponse response, String errMsg, HttpStatus httpStatus) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(httpStatus);
    }
    @Override
    public ResponseEntity<Resource> downloadFile(String fileName, String rootOrgId, String userAuthToken) {
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                logger.error("Not able to get userId from authToken ");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
            }
            if (!validateUserOrgId(rootOrgId, userId)) {
                logger.error("User is not authorized to download the file for other org");
                return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
            }
            storageService.downloadFile(fileName, serverProperties.getOrgHierarchyBulkUploadContainerName());
            Path tmpPath = Paths.get(Constants.LOCAL_BASE_PATH + fileName);
            ByteArrayResource resource = new ByteArrayResource(Files.readAllBytes(tmpPath));
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + fileName + "\"");
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(tmpPath.toFile().length())
                    .contentType(MediaType.parseMediaType(MediaType.MULTIPART_FORM_DATA_VALUE))
                    .body(resource);
        } catch (IOException e) {
            logger.error("Failed to read the downloaded file: " + fileName + ", Exception: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            try {
                File file = new File(Constants.LOCAL_BASE_PATH + fileName);
                if (file.exists()) {
                    file.delete();
                }
            } catch (Exception e1) {
            }
        }
    }

    @Override
    public SBApiResponse getBulkUploadDetailsForOrgHierarchyMapping(String orgId, String rootOrgId, String userAuthToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_HIERARCHY_BULK_UPLOAD_STATUS);
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            if (StringUtils.isNotBlank(orgId)) {
                propertyMap.put(Constants.ROOT_ORG_ID, orgId);
            }
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }
            if (!validateUserOrgId(rootOrgId, userId)) {
                logger.error("User is not authorized to get the fileInfo for other org: " + rootOrgId + ", request orgId " + orgId);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("User is not authorized to get the fileInfo for other org");
                response.setResponseCode(HttpStatus.UNAUTHORIZED);
                return response;
            }
            List<Map<String, Object>> bulkUploadList = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                    Constants.ORG_HIERARCHY_MAPPING_BULK_UPLOAD, propertyMap, serverProperties.getBulkUploadStatusFields());
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.CONTENT, bulkUploadList);
            response.getResult().put(Constants.COUNT, bulkUploadList != null ? bulkUploadList.size() : 0);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to get user bulk upload request status. Error: ", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private boolean validateUserOrgId(String orgId, String userId) {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        userUtilityService.getUserDetailsFromDB(Arrays.asList(userId), Arrays.asList(Constants.USER_ID, Constants.ROOT_ORG_ID, Constants.CHANNEL), userInfoMap);
        if (MapUtils.isNotEmpty(userInfoMap)) {
            String rootOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
            String channel = userInfoMap.get(userId).get(Constants.CHANNEL);

            // Adding the condition for spv and also for Mdo OrgId
            return (StringUtils.equalsIgnoreCase(serverProperties.getSpvChannelName(), channel) || StringUtils.equalsIgnoreCase(orgId, rootOrgId));
        }
        return false;
    }

}