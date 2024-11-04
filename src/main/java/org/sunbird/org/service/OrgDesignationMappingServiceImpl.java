package org.sunbird.org.service;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.xssf.usermodel.*;
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
import org.sunbird.org.model.CustomResponseDTO;
import org.sunbird.org.model.Data;
import org.sunbird.org.model.NestedResult;
import org.sunbird.storage.service.StorageServiceImpl;
import org.sunbird.user.service.UserUtilityService;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class OrgDesignationMappingServiceImpl implements OrgDesignationMappingService {

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;

    @Autowired
    private CbExtServerProperties serverProperties;

    private final Logger logger = LoggerFactory.getLogger(OrgDesignationMappingServiceImpl.class);

    @Autowired
    StorageServiceImpl storageService;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    private UserUtilityService userUtilityService;

    @Autowired
    RedisCacheMgr redisCache;

    /**
     * @param rootOrgId
     * @param userAuthToken
     * @param frameworkId
     * @return
     */
    @Override
    public ResponseEntity<?> getSampleFileForOrgDesignationMapping(String rootOrgId, String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            // Create sheets with safe names
            Sheet yourWorkspaceSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadCompetencyYourWorkSpaceName()));
            Sheet designationMasterSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getSampleFileMasterDesignationWorkSpaceName()));

            // Headers for all sheets
            String[] headersWorksheet = {"Designation"};
            String[] headersDesignation = {"Designation"};
            // Create header rows in each sheet

            createHeaderRowForYourWorkBook(yourWorkspaceSheet, headersWorksheet);
            createHeaderRow(designationMasterSheet, headersDesignation);

            // Example data (can be replaced with actual data)
            populateDesignationMaster(designationMasterSheet);

            makeSheetReadOnly(designationMasterSheet);

            // Set up the dropdowns in "Your Workspace" sheet
            setUpDropdowns(workbook, yourWorkspaceSheet, designationMasterSheet);

            // Set column widths to avoid cell overlap
            setColumnWidths(designationMasterSheet);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            workbook.close();

            // Convert the output stream to a byte array and return as a downloadable file
            ByteArrayResource resource = new ByteArrayResource(outputStream.toByteArray());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverProperties.getSampleBulkUploadCompetencyDesignationFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            logger.error("Issue while downloading the sample file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    /**
     * @param file
     * @param rootOrgId
     * @param userAuthToken
     * @param frameworkId
     * @return
     */
    @Override
    public SBApiResponse bulkUploadDesignationMapping(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_DESIGNATION_EVENT_BULK_UPLOAD);
        try {
            String userId = validateAuthTokenAndFetchUserId(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getOrgDesignationBulkUploadContainerName());
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                setErrorData(response, String.format("Failed to upload file. Error: %s",
                        (String) uploadResponse.getParams().getErrmsg()), HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            HashMap<String, Object> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.ROOT_ORG_ID, rootOrgId);
            uploadedFile.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
            uploadedFile.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
            uploadedFile.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
            uploadedFile.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
            uploadedFile.put(Constants.STATUS, Constants.INITIATED_CAPITAL);
            uploadedFile.put(Constants.CREATED_BY, userId);

            SBApiResponse insertResponse = cassandraOperation.insertRecord(Constants.DATABASE,
                    Constants.TABLE_ORG_DESIGNATION_MAPPING_BULK_UPLOAD, uploadedFile);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with org Designation bulk details.", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(uploadedFile);
            uploadedFile.put(Constants.X_AUTH_TOKEN, userAuthToken);
            uploadedFile.put(Constants.FRAMEWORK_ID, frameworkId);

            kafkaProducer.pushWithKey(serverProperties.getOrgDesignationBulkUploadTopic(), uploadedFile, rootOrgId);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to process Org Designation bulk upload request. Error: ", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private List<Map<String, Object>> populateMasterDesignation() {
        try {
            XmlMapper xmlMapper = new XmlMapper();
            xmlMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            String masterData = redisCache.getCache(Constants.DESIGNATION_MASTER_DATA);
            if (StringUtils.isEmpty(masterData)) {
                Map<String, Object> searchRequest = new HashMap<>();

                searchRequest.put(Constants.PAGE_NUMBER, 0);
                searchRequest.put(Constants.PAGE_SIZE, 10000);
                searchRequest.put(Constants.REQUEST_FIELDS, new ArrayList<>());

                Map<String, Object> filterCriteriaMap = new HashMap<>();
                filterCriteriaMap.put(Constants.STATUS, "Active");

                searchRequest.put(Constants.FILTER_CRITERIA_MAP, filterCriteriaMap);

                String url = serverProperties.getCbPoresServiceHost() + serverProperties.getCbPoresMasterDesignationEndpoint();
                String masterDesignationString = (String) outboundRequestHandler.fetchResultUsingPostAsString(
                        url, searchRequest);
                CustomResponseDTO responseDTO = xmlMapper.readValue(masterDesignationString, CustomResponseDTO.class);
                List<Data> dataList = null;
                if (responseDTO != null && responseDTO.getResult() != null) {
                    NestedResult nestedResult = responseDTO.getResult().getResult();
                    if (nestedResult != null) {
                        dataList = nestedResult.getData();
                        String jsonString = objectMapper.writeValueAsString(dataList);
                        List<Map<String, Object>> masterDataNode = objectMapper.readValue(jsonString, new TypeReference<List<Map<String, Object>>>() {
                        });
                        redisCache.putCache(Constants.DESIGNATION_MASTER_DATA, masterDataNode, serverProperties.getRedisMasterDataReadTimeOut());
                        return masterDataNode;
                    }
                }
            } else {
                return objectMapper.readValue(masterData, new TypeReference<List<Map<String, Object>>>() {
                });
            }
        } catch (IOException e) {
            logger.error("Error while converting the response ", e);
        }
        return null;
    }

    private static void createHeaderRow(Sheet sheet, String[] headers) {
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
        }
        sheet.createFreezePane(0, 1);
    }

    private void createHeaderRowForYourWorkBook(Sheet sheet, String[] headers) {
        Row headerRow = sheet.createRow(0);

        CellStyle headerStyle = sheet.getWorkbook().createCellStyle();
        Font font = sheet.getWorkbook().createFont();
        font.setBold(true);  // Make the header font bold
        headerStyle.setFont(font);
        headerStyle.setLocked(true);  // Lock the header cells

        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);  // Apply the locked header style

            // Auto-size the column based on header content
            sheet.autoSizeColumn(i);

            // Set a minimum fixed column width if autoSize doesn't provide enough space
            int width = sheet.getColumnWidth(i);
            if (width < 40 * 256) {  // Set a minimum width of 40 characters
                sheet.setColumnWidth(i, 40 * 256);
            }
        }

        // Freeze the header row so it stays visible when scrolling
        sheet.createFreezePane(0, 1);  // Freeze the first row (index 0)

        // Protect the sheet so that only locked cells (header row) are protected
        sheet.protectSheet("");  // Protect the sheet without a password

        // Create an unlocked style (all cells will be unlocked by default)
        CellStyle unlockedStyle = sheet.getWorkbook().createCellStyle();
        unlockedStyle.setLocked(false);

        // Set the default column style for the rest of the sheet to be unlocked
        for (int colIdx = 0; colIdx < headers.length; colIdx++) {
            sheet.setDefaultColumnStyle(colIdx, unlockedStyle);
        }
    }

    private void populateDesignationMaster(Sheet sheet) throws Exception {
        List<Map<String, Object>> getAllDesignationForOrg = populateMasterDesignation();
        if (CollectionUtils.isNotEmpty(getAllDesignationForOrg)) {
            List<String> designations = getMasterDesignation(getAllDesignationForOrg);
            for (int i = 0; i < designations.size(); i++) {
                Row row = sheet.createRow(i + 1);  // Create a new row starting from row 2 (index 1)
                Cell cell = row.createCell(0);  // Create a new cell in the first column
                cell.setCellValue(designations.get(i));
            }
        } else {
            throw new Exception("Issue while populating the master data");
        }
    }

    private List<String> getMasterDesignation(List<Map<String, Object>> getAllDesignationForOrg) {
        List<String> designation = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(getAllDesignationForOrg)) {
            designation = getAllDesignationForOrg.stream()
                    .filter(map -> StringUtils.isNotBlank((String) map.get(Constants.DESIGNATION)))
                    .map(map -> (String) map.get(Constants.DESIGNATION))
                    .distinct()
                    .collect(Collectors.toList());
        }
        return designation;
    }

    private List<String> getOrgAddedDesignation(List<Map<String, Object>> getAllDesignationForOrg) {
        List<String> designation = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(getAllDesignationForOrg)) {
            Map<String, Object> designationFrameworkObject = getAllDesignationForOrg.stream().filter(n -> ((String) (n.get("code")))
                    .equalsIgnoreCase(Constants.DESIGNATION)).findFirst().orElse(null);
            if (MapUtils.isNotEmpty(designationFrameworkObject)) {
                List<Map<String, Object>> designationFrameworkTerms = (List<Map<String, Object>>) designationFrameworkObject.get("terms");
                if (CollectionUtils.isNotEmpty(designationFrameworkTerms)) {
                    designation = designationFrameworkTerms.stream()
                            .map(map -> (String) map.get(Constants.NAME))
                            .distinct()  // Ensure unique values
                            .collect(Collectors.toList());
                }
            }
        }
        return designation;
    }

    private Map<String, Object> getDesignationObject(List<Map<String, Object>> getAllDesignationForOrg, String designation) {
        if (CollectionUtils.isNotEmpty(getAllDesignationForOrg)) {
            Map<String, Object> designationFrameworkObject = getAllDesignationForOrg.stream().filter(n -> ((String) (n.get("code")))
                    .equalsIgnoreCase(Constants.DESIGNATION)).findFirst().orElse(null);
            if (MapUtils.isNotEmpty(designationFrameworkObject)) {
                List<Map<String, Object>> designationFrameworkTerms = (List<Map<String, Object>>) designationFrameworkObject.get("terms");
                if (CollectionUtils.isNotEmpty(designationFrameworkTerms)) {
                    return designationFrameworkTerms.stream().filter(n -> ((String) n.get(Constants.NAME)).equalsIgnoreCase(designation)).findFirst().orElse(null);
                }
            }
        }
        return null;
    }

    private void makeSheetReadOnly(Sheet sheet) {
        sheet.protectSheet(Constants.PASSWORD);  // Protect the sheet with a random UUID as the password
    }

    public void setUpDropdowns(Workbook workbook, Sheet yourWorkspaceSheet, Sheet designationMasterSheet) {
        XSSFDataValidationHelper validationHelper = new XSSFDataValidationHelper((XSSFSheet) yourWorkspaceSheet);

        // Dropdown for "Designation" column from "Designation master"
        String designationRange = "'" + designationMasterSheet.getSheetName() + "'!$A$2:$A$" + (designationMasterSheet.getLastRowNum() + 1);
        setDropdownForColumn(validationHelper, yourWorkspaceSheet, 0, designationRange);

    }

    private void setDropdownForColumn(XSSFDataValidationHelper validationHelper, Sheet targetSheet, int targetColumn, String range) {
        int firstRow = 1;
        int lastRow = 1000;
        if (lastRow < firstRow) {
            lastRow = firstRow; // Avoid invalid range if the sheet has no data
        }

        DataValidationConstraint constraint = null;
        CellRangeAddressList addressList = new CellRangeAddressList(firstRow, lastRow, targetColumn, targetColumn);
        constraint = validationHelper.createFormulaListConstraint("INDIRECT(\"" + range + "\")");
        DataValidation dataValidation = validationHelper.createValidation(constraint, addressList);

        if (dataValidation instanceof XSSFDataValidation) {
            dataValidation.setSuppressDropDownArrow(true);
            dataValidation.setShowErrorBox(true);
        }
        targetSheet.addValidationData(dataValidation);
    }

    private void setColumnWidths(Sheet sheet) {
        for (int i = 0; i < sheet.getRow(0).getPhysicalNumberOfCells(); i++) {
            sheet.autoSizeColumn(i);
        }
    }

    @Override
    public void initiateOrgDesignationBulkUploadProcess(String value) {
        logger.info("OrgDesignationMapping:: initiateUserBulkUploadProcess: Started");
        long duration = 0;
        long startTime = System.currentTimeMillis();
        try {
            HashMap<String, String> inputDataMap = objectMapper.readValue(value,
                    new TypeReference<Object>() {
                    });
            List<String> errList = validateReceivedKafkaMessage(inputDataMap);
            if (errList.isEmpty()) {
                updateOrgCompetencyDesignationMappingBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID),
                        inputDataMap.get(Constants.IDENTIFIER), Constants.STATUS_IN_PROGRESS_UPPERCASE, 0, 0, 0);
                storageService.downloadFile(inputDataMap.get(Constants.FILE_NAME), serverProperties.getOrgDesignationBulkUploadContainerName());
                processBulkUpload(inputDataMap);
            } else {
                logger.error(String.format("Error in the Kafka Message Received : %s", errList));
            }
        } catch (Exception e) {
            logger.error(String.format("Error in the scheduler to upload bulk users %s", e.getMessage()),
                    e);
        }
        duration = System.currentTimeMillis() - startTime;
        logger.info("CompetencyDesignationMapping:: initiateUserBulkUploadProcess: Completed. Time taken: "
                + duration + " milli-seconds");
    }

    /**
     * @param orgId
     * @return
     */
    @Override
    public SBApiResponse getBulkUploadDetailsForOrgDesignationMapping(String orgId, String rootOrgId, String userAuthToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_DESIGNATION_BULK_UPLOAD_STATUS);
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
                    Constants.TABLE_ORG_DESIGNATION_MAPPING_BULK_UPLOAD, propertyMap, serverProperties.getBulkUploadStatusFields());
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

    /**
     * @param fileName
     * @return
     */
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
            storageService.downloadFile(fileName, serverProperties.getOrgDesignationBulkUploadContainerName());
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

    private void setErrorData(SBApiResponse response, String errMsg, HttpStatus httpStatus) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(httpStatus);
    }

    private String validateAuthTokenAndFetchUserId(String authUserToken) {
        return accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
    }

    public void updateOrgCompetencyDesignationMappingBulkUploadStatus(String rootOrgId, String identifier, String status, int totalRecordsCount,
                                                                      int successfulRecordsCount, int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);
            Map<String, Object> fieldsToBeUpdated = new HashMap<>();
            if (!status.isEmpty()) {
                fieldsToBeUpdated.put(Constants.STATUS, status);
            }
            if (totalRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.TOTAL_RECORDS, totalRecordsCount);
            }
            if (successfulRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.SUCCESSFUL_RECORDS_COUNT, successfulRecordsCount);
            }
            if (failedRecordsCount >= 0) {
                fieldsToBeUpdated.put(Constants.FAILED_RECORDS_COUNT, failedRecordsCount);
            }
            fieldsToBeUpdated.put(Constants.DATE_UPDATE_ON, new Timestamp(System.currentTimeMillis()));
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_ORG_DESIGNATION_MAPPING_BULK_UPLOAD,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error(String.format("Error in Updating  Bulk Upload Status in Cassandra %s", e.getMessage()), e);
        }
    }

    private void processBulkUpload(HashMap<String, String> inputDataMap) throws IOException {
        File file = null;
        FileInputStream fis = null;
        XSSFWorkbook wb = null;
        int totalRecordsCount = 0;
        int noOfSuccessfulRecords = 0;
        int failedRecordsCount = 0;
        int totalNumberOfRecordInSheet = 0;
        int progressUpdateThresholdValue = 0;
        String status = "";
        List<String> masterDesignation = new ArrayList<>();
        List<Map<String, Object>> masterDesignationMapping = populateMasterDesignation();
        if (CollectionUtils.isNotEmpty(masterDesignationMapping)) {
            masterDesignation = getMasterDesignation(masterDesignationMapping);
        }
        String userId = inputDataMap.get(Constants.CREATED_BY);
        Map<String, String> userInfoMap = new HashMap<>();
        if (StringUtils.isNotEmpty(userId)) {
            userInfoMap = userInfoMap(userId);
        }
        try {
            file = new File(Constants.LOCAL_BASE_PATH + inputDataMap.get(Constants.FILE_NAME));
            if (file.exists() && file.length() > 0) {
                fis = new FileInputStream(file);
                wb = new XSSFWorkbook(fis);
                XSSFSheet sheet = wb.getSheetAt(0);

                Iterator<Row> rowIterator = sheet.iterator();
                String frameworkId = inputDataMap.get(Constants.FRAMEWORK_ID);
                List<String> orgDesignation = null;
                // incrementing the iterator inorder to skip the headers in the first row
                if (rowIterator.hasNext()) {
                    Row firstRow = rowIterator.next();
                    Cell statusCell = firstRow.getCell(1);
                    Cell errorDetails = firstRow.getCell(2);
                    if (statusCell == null) {
                        statusCell = firstRow.createCell(1);
                    }
                    if (errorDetails == null) {
                        errorDetails = firstRow.createCell(2);
                    }
                    statusCell.setCellValue("Status");
                    errorDetails.setCellValue("Error Details");
                }
                int count = 0;
                while (rowIterator.hasNext()) { // to get the totalNumber of record to get the rows which have data
                    Row nextRow = rowIterator.next();
                    boolean allColumnsEmpty = true;
                    for (int colIndex = 0; colIndex < 1; colIndex++) { // Only check the first 4 columns
                        Cell cell = nextRow.getCell(colIndex);

                        if (cell != null) {
                            // Check if the cell is not blank or doesn't contain an empty string
                            if (!(cell.getCellType() == CellType.BLANK ||
                                    (cell.getCellType() == CellType.STRING && cell.getStringCellValue().trim().isEmpty()))) {
                                allColumnsEmpty = false;
                                break; // Stop checking further if any cell has data
                            }
                        }
                    }
                    if (allColumnsEmpty) continue;
                    totalNumberOfRecordInSheet++;
                }
                rowIterator = sheet.iterator();
                if (rowIterator.hasNext()) {
                    Row firstRow = rowIterator.next();
                    Cell statusCell = firstRow.getCell(1);
                    Cell errorDetails = firstRow.getCell(2);
                    if (statusCell == null) {
                        statusCell = firstRow.createCell(1);
                    }
                    if (errorDetails == null) {
                        errorDetails = firstRow.createCell(2);
                    }
                    statusCell.setCellValue("Status");
                    errorDetails.setCellValue("Error Details");
                }
                while (rowIterator.hasNext()) {
                    Row nextRow = rowIterator.next();
                    boolean allColumnsEmpty = true;
                    for (int colIndex = 0; colIndex < 1; colIndex++) { // Only check the first 4 columns
                        Cell cell = nextRow.getCell(colIndex);

                        if (cell != null) {
                            // Check if the cell is not blank or doesn't contain an empty string
                            if (!(cell.getCellType() == CellType.BLANK ||
                                    (cell.getCellType() == CellType.STRING && cell.getStringCellValue().trim().isEmpty()))) {
                                allColumnsEmpty = false;
                                break; // Stop checking further if any cell has data
                            }
                        }
                    }
                    if (allColumnsEmpty) continue;
                    logger.info("CompetencyDesignationMapping:: Record " + count++);
                    List<Map<String, Object>> getAllDesignationForOrg = populateDataFromFrameworkTerm(frameworkId);
                    Map<String, Object> orgFrameworkObject = null;
                    List<Map<String, Object>> orgFrameworkTerms = null;
                    if (CollectionUtils.isNotEmpty(getAllDesignationForOrg)) {
                        orgFrameworkObject = getAllDesignationForOrg.stream().filter(n -> ((String) (n.get("code")))
                                .equalsIgnoreCase(Constants.ORG)).findFirst().orElse(null);
                        if (MapUtils.isNotEmpty(orgFrameworkObject)) {
                            orgFrameworkTerms = (List<Map<String, Object>>) orgFrameworkObject.get("terms");
                        }
                    }

                    if (StringUtils.isNotEmpty(frameworkId)) {
                        orgDesignation = getOrgAddedDesignation(getAllDesignationForOrg);
                    }
                    long duration = 0;
                    long startTime = System.currentTimeMillis();
                    StringBuffer str = new StringBuffer();
                    List<String> errList = new ArrayList<>();
                    List<String> invalidErrList = new ArrayList<>();
                    Map<String, Object> designationMappingInfoMap = new HashMap<>();
                    String associationIdentifier = "";
                    designationMappingInfoMap.put(Constants.USER, userInfoMap);
                    if (nextRow.getCell(0) == null || nextRow.getCell(0).getCellType() == CellType.BLANK) {
                        errList.add("Designation");
                    } else {
                        if (nextRow.getCell(0).getCellType() == CellType.STRING) {
                            String designation = nextRow.getCell(0).getStringCellValue().trim();
                            if (CollectionUtils.isNotEmpty(orgDesignation) && orgDesignation.contains(designation)) {
                                if (CollectionUtils.isNotEmpty(orgFrameworkTerms)) {
                                    List<Map<String, Object>> associations = (List<Map<String, Object>>) orgFrameworkTerms.get(0).get(Constants.ASSOCIATIONS);
                                    if (CollectionUtils.isNotEmpty(associations)) {
                                        boolean isDesignationAssociationsPresent = associations.stream().anyMatch(n -> ((String) n.get(Constants.NAME)).equalsIgnoreCase(designation));
                                        if (isDesignationAssociationsPresent) {
                                            invalidErrList.add("Already designation: " + designation + "is mapped for org");
                                        } else {
                                            Map<String, Object> designationObject = getDesignationObject(getAllDesignationForOrg, designation);
                                            if (MapUtils.isNotEmpty(designationObject)) {
                                                associationIdentifier = (String) designationObject.get(Constants.IDENTIFIER);
                                            } else {
                                                invalidErrList.add("Issue while adding the designation for org");
                                            }
                                        }
                                    }
                                }
                            } else {
                                if (masterDesignation.contains(designation)) {
                                    designationMappingInfoMap.put(Constants.DESIGNATION, designation);
                                } else {
                                    invalidErrList.add("Invalid designation for org: " + designation);
                                }
                            }

                        } else {
                            invalidErrList.add("Invalid column type for designation. Expecting string format");
                        }
                    }

                    Cell statusCell = nextRow.getCell(1);
                    Cell errorDetails = nextRow.getCell(2);
                    if (statusCell == null) {
                        statusCell = nextRow.createCell(1);
                    }
                    if (errorDetails == null) {
                        errorDetails = nextRow.createCell(2);
                    }
                    if (totalRecordsCount == 0 && errList.size() == 1) {
                        setErrorDetails(str, errList, statusCell, errorDetails);
                        failedRecordsCount++;
                    }
                    totalRecordsCount++;
                    progressUpdateThresholdValue++;
                    String orgId = inputDataMap.get(Constants.ROOT_ORG_ID);
                    if (!errList.isEmpty()) {
                        setErrorDetails(str, errList, statusCell, errorDetails);
                        failedRecordsCount++;
                    } else {
                        if (CollectionUtils.isNotEmpty(orgFrameworkTerms)) {
                            Map<String, Object> orgFrameworkTerm = orgFrameworkTerms.stream().filter(n -> ((String) n.get(Constants.STATUS)).equalsIgnoreCase(Constants.LIVE)).findFirst().map(HashMap::new).orElse(null);
                            if (MapUtils.isNotEmpty(orgFrameworkTerm)) {
                                designationMappingInfoMap.put(Constants.ORGANISATION, orgFrameworkTerm);

                            } else {
                                invalidErrList.add("The issue while fetching the framework term for the org which is active.");
                            }
                        }
                        if (CollectionUtils.isNotEmpty(masterDesignationMapping) && StringUtils.isEmpty(associationIdentifier) && CollectionUtils.isEmpty(invalidErrList)) {
                            Map<String, Object> masterObjectForDesignation = masterDesignationMapping.stream().filter(n -> ((String) n.get(Constants.DESIGNATION)).equalsIgnoreCase((String) designationMappingInfoMap.get(Constants.DESIGNATION))).findFirst().map(HashMap::new).orElse(null);
                            if (MapUtils.isNotEmpty(masterObjectForDesignation)) {
                                designationMappingInfoMap.put(Constants.DESIGNATION, masterObjectForDesignation);
                                addUpdateDesignationMapping(frameworkId, designationMappingInfoMap, invalidErrList, orgId, null);
                            } else {
                                invalidErrList.add("The issue while fetching the framework term for the org.");
                            }
                        } else {
                            if (StringUtils.isNotEmpty(associationIdentifier)) {
                                addUpdateDesignationMapping(frameworkId, designationMappingInfoMap, invalidErrList, orgId, associationIdentifier);
                            }
                            if (StringUtils.isEmpty(associationIdentifier) && CollectionUtils.isEmpty(invalidErrList)) {
                                invalidErrList.add("The issue while fetching the framework for the org.");
                            }
                        }
                        if (CollectionUtils.isEmpty(invalidErrList)) {
                            noOfSuccessfulRecords++;
                            statusCell.setCellValue(Constants.SUCCESS_UPPERCASE);
                            errorDetails.setCellValue("");
                        } else {
                            failedRecordsCount++;
                            statusCell.setCellValue(Constants.FAILED_UPPERCASE);
                            errorDetails.setCellValue(String.join(", ", invalidErrList));
                        }
                    }
                    duration = System.currentTimeMillis() - startTime;
                    logger.info("UserBulkUploadService:: Record Completed. Time taken: "
                            + duration + " milli-seconds");
                    if (progressUpdateThresholdValue >= serverProperties.getBulkUploadThresholdValue()) {
                        updateOrgCompetencyDesignationMappingBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID), inputDataMap.get(Constants.IDENTIFIER),
                                Constants.STATUS_IN_PROGRESS_UPPERCASE, totalNumberOfRecordInSheet, noOfSuccessfulRecords, failedRecordsCount);
                        progressUpdateThresholdValue = 0;
                    }
                }
                if (totalRecordsCount == 0) {
                    XSSFRow row = sheet.createRow(sheet.getLastRowNum() + 1);
                    Cell statusCell = row.createCell(1);
                    Cell errorDetails = row.createCell(2);
                    statusCell.setCellValue(Constants.FAILED_UPPERCASE);
                    errorDetails.setCellValue(Constants.EMPTY_FILE_FAILED);

                }
                status = uploadTheUpdatedFile(file, wb);
                if (!(Constants.SUCCESSFUL.equalsIgnoreCase(status) && failedRecordsCount == 0
                        && totalRecordsCount == noOfSuccessfulRecords && totalRecordsCount >= 1)) {
                    status = Constants.FAILED_UPPERCASE;
                }
            } else {
                logger.info("Error in Process Bulk Upload : The File is not downloaded/present");
                status = Constants.FAILED_UPPERCASE;
            }
            updateOrgCompetencyDesignationMappingBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID), inputDataMap.get(Constants.IDENTIFIER),
                    status, totalRecordsCount, noOfSuccessfulRecords, failedRecordsCount);
        } catch (Exception e) {
            logger.error(String.format("Error in Process Bulk Upload %s", e.getMessage()), e);
            updateOrgCompetencyDesignationMappingBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID), inputDataMap.get(Constants.IDENTIFIER),
                    Constants.FAILED_UPPERCASE, 0, 0, 0);
        } finally {
            if (wb != null)
                wb.close();
            if (fis != null)
                fis.close();
            if (file != null)
                file.delete();
        }
    }

    private List<Map<String, Object>> populateDataFromFrameworkTerm(String frameworkId) throws InterruptedException {
        Thread.sleep(500);
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
        String url = serverProperties.getKmBaseHost() + serverProperties.getKmFrameWorkPath() + "/" + frameworkId;
        Map<String, Object> termFrameworkCompetencies = (Map<String, Object>) outboundRequestHandler.fetchUsingGetWithHeaders(
                url, headers);
        if (MapUtils.isNotEmpty(termFrameworkCompetencies)) {
            Map<String, Object> result = ((Map<String, Object>) termFrameworkCompetencies.get(Constants.RESULT));
            if (MapUtils.isNotEmpty(result)) {
                Map<String, Object> frameworkObject = ((Map<String, Object>) result.get(Constants.FRAMEWORK));
                if (MapUtils.isNotEmpty(frameworkObject)) {
                    return (List<Map<String, Object>>) frameworkObject.get(Constants.CATEGORIES);
                }
            }
        }
        return null;
    }

    private void addUpdateDesignationMapping(String frameworkId, Map<String, Object> designationMappingInfoMap, List<String> invalidErrList, String orgId, String associationsIdentifier) {
        try {
            List<String> nodeId = new ArrayList<>();
            if (StringUtils.isEmpty(associationsIdentifier)) {
                Map<String, Object> termCreateRespDesignation = createTermFrameworkObjectForDesignation(frameworkId, designationMappingInfoMap);
                if (MapUtils.isNotEmpty(termCreateRespDesignation)) {
                    nodeId.addAll((List<String>) termCreateRespDesignation.get("node_id"));
                    logger.info("The NodeId for term create is: " + nodeId);
                }
            } else {
                nodeId.add(associationsIdentifier);
            }

            List<String> associations = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(nodeId)) {
                Map<String, Object> terms = (Map<String, Object>) designationMappingInfoMap.get(Constants.ORGANISATION);
                if (MapUtils.isNotEmpty(terms)) {
                    List<Map<String, Object>> associationsMap = (List<Map<String, Object>>) terms.get(Constants.ASSOCIATIONS);
                    if (CollectionUtils.isNotEmpty(associationsMap)) {
                        associations.addAll(associationsMap.stream().map(n -> (String) n.get(Constants.IDENTIFIER)).collect(Collectors.toList()));
                        associations.addAll(nodeId);
                        List<Map<String, Object>> createDesignationObject = new ArrayList<>();
                        for (String association : associations) {
                            Map<String, Object> nodeIdMap = new HashMap<>();
                            nodeIdMap.put(Constants.IDENTIFIER, association);
                            createDesignationObject.add(nodeIdMap);
                        }
                        logger.info("The associated size need to be updated: " + associations.size());
                        Map<String, Object> frameworkAssociationUpdateForOrg = updateFrameworkTerm(frameworkId, updateRequestObject(createDesignationObject), Constants.ORG, (String) terms.get(Constants.CODE));
                        if (MapUtils.isNotEmpty(frameworkAssociationUpdateForOrg)) {
                            Map<String, Object> result = publishFramework(frameworkId, new HashMap<>(), orgId);
                            if (MapUtils.isNotEmpty(result)) {
                                logger.info("Publish is Success for frameworkId: " + frameworkId);
                            } else {
                                invalidErrList.add("Issue while publish the framework.");
                            }
                        } else {
                            invalidErrList.add("Issue while adding the associations to the framework for theme.");
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Issue while creating the term object for designation.", e);
            invalidErrList.add("Issue while creating the term object for designation.");
        }
    }

    private void setErrorDetails(StringBuffer str, List<String> errList, Cell statusCell, Cell errorDetails) {
        str.append("Failed to process user record. Missing Parameters - ").append(errList);
        statusCell.setCellValue(Constants.FAILED_UPPERCASE);
        errorDetails.setCellValue(str.toString());
    }

    private String uploadTheUpdatedFile(File file, XSSFWorkbook wb)
            throws IOException {
        FileOutputStream fileOut = new FileOutputStream(file);
        wb.write(fileOut);
        fileOut.close();
        SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getOrgDesignationBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.info(String.format("Failed to upload file. Error: %s",
                    uploadResponse.getParams().getErrmsg()));
            return Constants.FAILED_UPPERCASE;
        }
        return Constants.SUCCESSFUL_UPPERCASE;
    }

    private List<String> validateReceivedKafkaMessage(HashMap<String, String> inputDataMap) {
        StringBuilder str = new StringBuilder();
        List<String> errList = new ArrayList<>();
        if (StringUtils.isEmpty(inputDataMap.get(Constants.ROOT_ORG_ID))) {
            errList.add("RootOrgId is not present");
        }
        if (org.apache.commons.lang.StringUtils.isEmpty(inputDataMap.get(Constants.IDENTIFIER))) {
            errList.add("Identifier is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.FILE_NAME))) {
            errList.add("Filename is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.X_AUTH_TOKEN))) {
            errList.add("User Token is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.FRAMEWORK_ID))) {
            errList.add("Framework ID is not present");
        }
        if (!errList.isEmpty()) {
            str.append("Failed to Validate User Details. Error Details - [").append(errList.toString()).append("]");
        }
        return errList;
    }

    private Map<String, Object> createTermFrameworkObjectForDesignation(String frameworkId, Map<String, Object> designationMappingObject) throws Exception {
        Map<String, Object> designationTermObject = designationTermObject(designationMappingObject);
        return createFrameworkTerm(frameworkId, designationTermObject, Constants.DESIGNATION);
    }

    private Map<String, Object> designationTermObject(Map<String, Object> designationMappingObject) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.CODE, UUIDs.timeBased());
        termReq.put(Constants.CATEGORY, Constants.DESIGNATION);
        Map<String, Object> designationMap = (Map<String, Object>) designationMappingObject.get(Constants.DESIGNATION);

        termReq.put(Constants.NAME, designationMap.get(Constants.DESIGNATION));
        termReq.put(Constants.DESCRIPTION, designationMap.get(Constants.DESCRIPTION));
        termReq.put(Constants.REFID, designationMap.get(Constants.ID));
        termReq.put(Constants.REF_TYPE, Constants.DESIGNATION);
        Map<String, Object> orgObject = (Map<String, Object>) designationMappingObject.get(Constants.ORGANISATION);
        Map<String, String> userObject = (Map<String, String>) designationMappingObject.get(Constants.USER);
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(Constants.IMPORT_BY_ID, userObject.get(Constants.USER_ID));
        additionalProperties.put(Constants.IMPORT_BY_NAME, userObject.get(Constants.FIRSTNAME));
        additionalProperties.put(Constants.PREVIOUS_TERM_CODE, orgObject.get(Constants.CODE));
        additionalProperties.put(Constants.PREVIOUS_CATEGORY_CODE, Constants.ORG);
        Instant now = Instant.now();
        ZonedDateTime zonedDateTime = now.atZone(ZoneId.systemDefault());
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd MMM, yyyy");
        String formattedDate = zonedDateTime.format(formatter);
        additionalProperties.put(Constants.IMPORTED_ON, formattedDate);
        additionalProperties.put(Constants.TIMESTAMP, now.toEpochMilli());
        termReq.put(Constants.ADDITIONAL_PROPERTIES, additionalProperties);

        requestBody.put(Constants.TERM, termReq);
        Map<String, Object> createReq = new HashMap<>();
        createReq.put(Constants.REQUEST, requestBody);
        return createReq;
    }

    private Map<String, Object> updateRequestObject(List<Map<String, Object>> associations) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.ASSOCIATIONS, associations);
        requestBody.put(Constants.TERM, termReq);
        Map<String, Object> createReq = new HashMap<>();
        createReq.put(Constants.REQUEST, requestBody);
        return createReq;
    }

    private Map<String, Object> createFrameworkTerm(String frameworkId, Map<String, Object> createReq, String category) throws Exception {
        StringBuilder strUrl = new StringBuilder(serverProperties.getKmBaseHost());
        strUrl.append(serverProperties.getKmFrameworkTermCreatePath()).append("?framework=")
                .append(frameworkId).append("&category=")
                .append(category);
        Map<String, Object> termResponse = outboundRequestHandler.fetchResultUsingPost(
                strUrl.toString(),
                createReq, null);
        if (termResponse != null
                && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
            logger.info("Created framework Term successfully");
            Map<String, Object> result = (Map<String, Object>) termResponse.get(Constants.RESULT);
            return result;
        } else {
            logger.error("Failed to create the competencyTheme object: " + objectMapper.writeValueAsString(createReq));
        }
        return null;
    }

    private Map<String, Object> updateFrameworkTerm(String frameworkId, Map<String, Object> createReq, String category, String code) throws Exception {
        StringBuilder strUrl = new StringBuilder(serverProperties.getKmBaseHost());
        strUrl.append(serverProperties.getKmFrameworkTermUpdatePath() + "/" + code).append("?framework=")
                .append(frameworkId).append("&category=")
                .append(category);
        Map<String, Object> termResponse = outboundRequestHandler.fetchResultUsingPatch(
                strUrl.toString(),
                createReq, null);
        if (termResponse != null
                && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
            logger.info("Updated framework Term successfully");
            Map<String, Object> result = (Map<String, Object>) termResponse.get(Constants.RESULT);
            return result;
        } else {
            logger.error("Failed to update the framework object: " + objectMapper.writeValueAsString(createReq));
        }
        return null;
    }

    private Map<String, Object> publishFramework(String frameworkId, Map<String, Object> createReq, String orgId) throws Exception {
        StringBuilder strUrl = new StringBuilder(serverProperties.getKmBaseHost());
        strUrl.append(serverProperties.getKmFrameworkPublishPath() + "/" + frameworkId);
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.X_CHANNEL_ID, orgId);

        Map<String, Object> termResponse = outboundRequestHandler.fetchResultUsingPost(
                strUrl.toString(),
                "", headers);
        if (termResponse != null
                && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
            logger.info("Published Framework : " + frameworkId);
            Map<String, Object> result = (Map<String, Object>) termResponse.get(Constants.RESULT);
            return result;
        } else {
            logger.error("Failed to publish the framework: " + objectMapper.writeValueAsString(createReq));
        }
        return null;
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

    private Map<String, String> userInfoMap(String userId) {
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        userUtilityService.getUserDetailsFromDB(Arrays.asList(userId), Arrays.asList(Constants.USER_ID, Constants.FIRSTNAME), userInfoMap);
        if (MapUtils.isNotEmpty(userInfoMap)) {
            return userInfoMap.get(userId);
        }
        return null;
    }
}
