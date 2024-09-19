package org.sunbird.org.service;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.sunbird.storage.service.StorageServiceImpl;
import org.sunbird.user.service.UserUtilityService;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class OrgDesignationCompetencyMappingServiceImpl implements OrgDesignationCompetencyMappingService {

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;

    @Autowired
    private CbExtServerProperties serverProperties;

    private final Logger logger = LoggerFactory.getLogger(OrgDesignationCompetencyMappingServiceImpl.class);

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
    RedisCacheMgr redisCacheMgr;

    @Override
    public ResponseEntity<ByteArrayResource> bulkUploadOrganisationCompetencyMapping(String rootOrgId, String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            // Create sheets with safe names
            Sheet yourWorkspaceSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadCompetencyYourWorkSpaceName()));
            Sheet referenceSheetCompetency = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadCompetencyReferenceWorkSpaceName()));
            Sheet orgDesignationMasterSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadMasterDesignationWorkSpaceName()));

            // Headers for all sheets
            String[] headersWorksheet = {"Designation", "Competency Area", "Competency Theme", "Competency SubTheme"};
            String[] headersCompetency = {"Competency Area", "Competency Theme", "Competency SubTheme"};
            String[] headersDesignation = {"Designation"};
            // Create header rows in each sheet

            createHeaderRowForYourWorkBook(yourWorkspaceSheet, headersWorksheet);
            createHeaderRow(referenceSheetCompetency, headersCompetency);
            createHeaderRow(orgDesignationMasterSheet, headersDesignation);

            // Example data (can be replaced with actual data)
            populateReferenceSheetCompetency(referenceSheetCompetency);
            populateOrgDesignationMaster(orgDesignationMasterSheet, frameworkId);

            makeSheetReadOnly(orgDesignationMasterSheet);
            makeSheetReadOnly(referenceSheetCompetency);

            // Set up the dropdowns in "Your Workspace" sheet
            setUpDropdowns(workbook, yourWorkspaceSheet, orgDesignationMasterSheet, referenceSheetCompetency);

            // Set column widths to avoid cell overlap
            //setColumnWidths(yourWorkspaceSheet);
            setColumnWidths(referenceSheetCompetency);
            setColumnWidths(orgDesignationMasterSheet);

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            workbook.write(outputStream);
            workbook.close();

            // Convert the output stream to a byte array and return as a downloadable file
            ByteArrayResource resource = new ByteArrayResource(outputStream.toByteArray());

            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"" + serverProperties.getBulkUploadCompetencyDesignationFileName() + "\"");

            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(resource.contentLength())
                    .contentType(org.springframework.http.MediaType.APPLICATION_OCTET_STREAM)
                    .body(resource);

        } catch (Exception e) {
            e.printStackTrace();
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
    public SBApiResponse bulkUploadCompetencyDesignationMapping(MultipartFile file, String rootOrgId, String userAuthToken, String frameworkId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMPETENCY_DESIGNATION_EVENT_BULK_UPLOAD);
        try {
            String userId = validateAuthTokenAndFetchUserId(userAuthToken);
            if (StringUtils.isBlank(userId)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getCompetencyDesignationBulkUploadContainerName());
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
                    Constants.TABLE_COMPETENCY_DESIGNATION_MAPPING_BULK_UPLOAD, uploadedFile);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with org competency Designation bulk details.", HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(uploadedFile);
            uploadedFile.put(Constants.X_AUTH_TOKEN, userAuthToken);
            uploadedFile.put(Constants.FRAMEWORK_ID, frameworkId);
            kafkaProducer.pushWithKey(serverProperties.getCompetencyDesignationBulkUploadTopic(), uploadedFile, rootOrgId);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to process Org competency Designation bulk upload request. Error: ", e.getMessage()), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private List<Map<String, Object>> populateDataFromFrameworkTerm(String frameworkId) {
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


    private void populateReferenceSheetCompetency(Sheet sheet) throws IOException {
        Set<String> competencyAreaSet = new LinkedHashSet<>();
        Set<String> competencyThemeSet = new LinkedHashSet<>();
        Set<String> competencySubThemeSet = new LinkedHashSet<>();
        int rowIndex = 1; // Start after header row
        List<Map<String, Object>> getAllCompetenciesMapping = getMasterCompetencyFrameworkData(serverProperties.getMasterCompetencyFrameworkName());

        if (CollectionUtils.isNotEmpty(getAllCompetenciesMapping)) {
            Map<String, Object> competencyAreaFrameworkObject = getAllCompetenciesMapping.stream().filter(n -> ((String) (n.get("code")))
                    .equalsIgnoreCase(Constants.COMPETENCY_AREA_LOWERCASE)).findFirst().orElse(null);
            Map<String, Object> competencyThemeFrameworkObject = getAllCompetenciesMapping.stream().filter(n -> ((String) (n.get("code")))
                    .equalsIgnoreCase(Constants.THEME)).findFirst().orElse(null);
            if (MapUtils.isNotEmpty(competencyAreaFrameworkObject) && MapUtils.isNotEmpty(competencyThemeFrameworkObject)) {
                List<Map<String, Object>> competencyAreaTerms = (List<Map<String, Object>>) competencyAreaFrameworkObject.get("terms");
                List<Map<String, Object>> competencyThemeTerms = (List<Map<String, Object>>) competencyThemeFrameworkObject.get("terms");
                if (CollectionUtils.isNotEmpty(competencyAreaTerms) && CollectionUtils.isNotEmpty(competencyThemeTerms)) {
                    for (Map<String, Object> competencyAreaTerm : competencyAreaTerms) {
                        String competencyArea = (String) competencyAreaTerm.get(Constants.NAME);
                        List<Map<String, Object>> competencyAreaAssociations = (List<Map<String, Object>>) competencyAreaTerm.get("associations");
                        for (Map<String, Object> competencyAreaAssociation : competencyAreaAssociations) {
                            String competencySubTheme = (String) competencyAreaAssociation.get(Constants.NAME);
                            String identifier = (String) competencyAreaAssociation.get(Constants.IDENTIFIER);
                            Map<String, Object> themeObject = competencyThemeTerms.stream().filter(theme -> ((String) theme.get(Constants.IDENTIFIER)).equalsIgnoreCase(identifier))
                                    .findFirst().orElse(null);
                            if (MapUtils.isNotEmpty(themeObject)) {
                                List<Map<String, Object>> themeObjectAssociations = (List<Map<String, Object>>) themeObject.get("associations");
                                if (CollectionUtils.isNotEmpty(themeObjectAssociations)) {
                                    for (Map<String, Object> themeAssociation : themeObjectAssociations) {
                                        String subTheme = (String) themeAssociation.get("name");
                                        if (competencyAreaSet.add(competencyArea) || competencyThemeSet.add(competencySubTheme) || competencySubThemeSet.add(subTheme)) {
                                            Row row = sheet.createRow(rowIndex++);
                                            row.createCell(0).setCellValue(competencyArea);
                                            row.createCell(1).setCellValue(competencySubTheme);
                                            row.createCell(2).setCellValue(subTheme);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private void populateOrgDesignationMaster(Sheet sheet, String frameworkId) {
        List<Map<String, Object>> getAllDesignationForOrg = populateDataFromFrameworkTerm(frameworkId);
        List<String> designations = getOrgAddedDesignation(getAllDesignationForOrg);
        for (int i = 0; i < designations.size(); i++) {
            Row row = sheet.createRow(i + 1);  // Create a new row starting from row 2 (index 1)
            Cell cell = row.createCell(0);  // Create a new cell in the first column
            cell.setCellValue(designations.get(i));
        }
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


    private void makeSheetReadOnly(Sheet sheet) {
        sheet.protectSheet(Constants.PASSWORD);  // Protect the sheet with a random UUID as the password
    }

    public void setUpDropdowns(Workbook workbook, Sheet yourWorkspaceSheet, Sheet orgDesignationMasterSheet, Sheet referenceSheetCompetency) {
        XSSFDataValidationHelper validationHelper = new XSSFDataValidationHelper((XSSFSheet) yourWorkspaceSheet);

        // Create or update the "NamedRanges" sheet
        createOrUpdateNamedRangesSheet(workbook, referenceSheetCompetency);

        // Dropdown for "Designation" column from "Org Designation master"
        String designationRange = "'" + orgDesignationMasterSheet.getSheetName() + "'!$A$2:$A$" + (orgDesignationMasterSheet.getLastRowNum() + 1);
        setDropdownForColumn(validationHelper, yourWorkspaceSheet, 0, designationRange);

        // Dropdown for "Competency Area" from named range
        String competencyAreaRange = "Competency_Areas";
        setDropdownForColumn(validationHelper, yourWorkspaceSheet, 1, competencyAreaRange);

        // Dropdown for "Competency Theme" depending on Competency Area
        setDependentDropdownForColumn(validationHelper, yourWorkspaceSheet, 2, "Themes_");

        // Dropdown for "Competency SubTheme" depending on Competency Theme
        setDependentDropdownForColumn(validationHelper, yourWorkspaceSheet, 3, "SubThemes_");
    }

    private void createOrUpdateNamedRangesSheet(Workbook workbook, Sheet referenceSheetCompetency) {
        Sheet namedRangesSheet = workbook.getSheet("NamedRanges");
        if (namedRangesSheet == null) {
            namedRangesSheet = workbook.createSheet("NamedRanges");
        } else {
            // Clear existing content
            int lastRowNum = namedRangesSheet.getLastRowNum();
            if (lastRowNum > 0) {
                for (int i = 0; i <= lastRowNum; i++) {
                    Row row = namedRangesSheet.getRow(i);
                    if (row != null) {
                        namedRangesSheet.removeRow(row);
                    }
                }
            }
        }

        // Create named ranges
        Map<String, Set<String>> themesMap = new LinkedHashMap<>();
        Map<String, Set<String>> subThemesMap = new LinkedHashMap<>();

        int lastRow = referenceSheetCompetency.getLastRowNum();
        for (int i = 1; i <= lastRow; i++) {
            Row row = referenceSheetCompetency.getRow(i);
            if (row == null) continue;

            String area = row.getCell(0).getStringCellValue();
            String theme = row.getCell(1).getStringCellValue();
            String subTheme = row.getCell(2).getStringCellValue();

            // Create named ranges for themes and subthemes
            themesMap.computeIfAbsent(area, k -> new LinkedHashSet<>()).add(theme);
            subThemesMap.computeIfAbsent(theme, k -> new LinkedHashSet<>()).add(subTheme);
        }

        // Write named ranges for Competency Area
        writeNamedRange(namedRangesSheet, "Competency_Areas", themesMap.keySet());

        // Write named ranges for Themes
        for (Map.Entry<String, Set<String>> entry : themesMap.entrySet()) {
            String area = entry.getKey();
            Set<String> themes = entry.getValue();
            writeNamedRange(namedRangesSheet, "Themes_" + makeNameSafe(area), themes);
        }

        // Write named ranges for SubThemes
        for (Map.Entry<String, Set<String>> entry : subThemesMap.entrySet()) {
            String theme = entry.getKey();
            Set<String> subThemes = entry.getValue();
            writeNamedRange(namedRangesSheet, "SubThemes_" + makeNameSafe(theme), subThemes);
        }

        makeSheetReadOnly(namedRangesSheet);
        workbook.setSheetHidden(workbook.getSheetIndex(namedRangesSheet), true);
    }

    private void writeNamedRange(Sheet namedRangesSheet, String name, Set<String> values) {
        int startRow = namedRangesSheet.getLastRowNum() + 1;
        for (String value : values) {
            Row row = namedRangesSheet.createRow(startRow++);
            row.createCell(0).setCellValue(value);
        }

        // Define named range
        Name namedRange = namedRangesSheet.getWorkbook().createName();
        namedRange.setNameName(name);
        namedRange.setRefersToFormula("'" + "NamedRanges" + "'!$A$" + (startRow - values.size() + 1) + ":$A$" + startRow);
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

    private void setDependentDropdownForColumn(XSSFDataValidationHelper validationHelper, Sheet targetSheet, int targetColumn, String dependentRangePrefix) {
        // Get the number of rows in the sheet
        int lastRow = targetSheet.getLastRowNum();

        // Optionally set a minimum number of rows to apply validation if the sheet is initially empty
        if (lastRow < 1) {
            lastRow = 1000; // or another reasonable default value
        }

        // Iterate over rows to apply data validation
        for (int rowIdx = 1; rowIdx <= lastRow; rowIdx++) {
            // Construct the formula for the data validation constraint using the prefix
            String formula = "INDIRECT(\"" + dependentRangePrefix + "\" & SUBSTITUTE($B" + (rowIdx + 1) + ", \" \", \"_\"))";
            if (dependentRangePrefix.equalsIgnoreCase("SubThemes_")) {
                formula = "INDIRECT(\"" + dependentRangePrefix + "\" & SUBSTITUTE($C" + (rowIdx + 1) + ", \" \", \"_\"))";
            }
            DataValidationConstraint constraint = validationHelper.createFormulaListConstraint(formula);

            // Define the range where the data validation should apply
            CellRangeAddressList addressList = new CellRangeAddressList(rowIdx, rowIdx, targetColumn, targetColumn);
            DataValidation dataValidation = validationHelper.createValidation(constraint, addressList);

            // Customize data validation settings if needed
            if (dataValidation instanceof XSSFDataValidation) {
                dataValidation.setSuppressDropDownArrow(true);
                dataValidation.setShowErrorBox(true);
            }

            // Add validation to the sheet
            targetSheet.addValidationData(dataValidation);
        }
    }


    private String makeNameSafe(String name) {
        return name.replaceAll("[^A-Za-z0-9_]", "_");
    }

    private void setColumnWidths(Sheet sheet) {
        for (int i = 0; i < sheet.getRow(0).getPhysicalNumberOfCells(); i++) {
            sheet.autoSizeColumn(i);
        }
    }

    @Override
    public void initiateCompetencyDesignationBulkUploadProcess(String value) {
        logger.info("CompetencyDesignationMapping:: initiateUserBulkUploadProcess: Started");
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
                storageService.downloadFile(inputDataMap.get(Constants.FILE_NAME), serverProperties.getCompetencyDesignationBulkUploadContainerName());
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
    public SBApiResponse getBulkUploadDetailsForCompetencyDesignationMapping(String orgId, String rootOrgId, String userAuthToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COMPETENCY_DESIGNATION_BULK_UPLOAD_STATUS);
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
                    Constants.TABLE_COMPETENCY_DESIGNATION_MAPPING_BULK_UPLOAD, propertyMap, serverProperties.getBulkUploadStatusFields());
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
            storageService.downloadFile(fileName, serverProperties.getCompetencyDesignationBulkUploadContainerName());
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
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_COMPETENCY_DESIGNATION_MAPPING_BULK_UPLOAD,
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
        List<Map<String, Object>> getAllCompetenciesMapping = getMasterCompetencyFrameworkData(serverProperties.getMasterCompetencyFrameworkName());
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
                    Cell statusCell = firstRow.getCell(4);
                    Cell errorDetails = firstRow.getCell(5);
                    if (statusCell == null) {
                        statusCell = firstRow.createCell(4);
                    }
                    if (errorDetails == null) {
                        errorDetails = firstRow.createCell(5);
                    }
                    statusCell.setCellValue("Status");
                    errorDetails.setCellValue("Error Details");
                }
                int count = 0;
                while (rowIterator.hasNext()) { // to get the totalNumber of record to get the rows which have data
                    Row nextRow = rowIterator.next();
                    boolean allColumnsEmpty = true;
                    for (int colIndex = 0; colIndex < 4; colIndex++) { // Only check the first 4 columns
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
                    Cell statusCell = firstRow.getCell(4);
                    Cell errorDetails = firstRow.getCell(5);
                    if (statusCell == null) {
                        statusCell = firstRow.createCell(4);
                    }
                    if (errorDetails == null) {
                        errorDetails = firstRow.createCell(5);
                    }
                    statusCell.setCellValue("Status");
                    errorDetails.setCellValue("Error Details");
                }
                while (rowIterator.hasNext()) {
                    List<Map<String, Object>> getAllDesignationForOrg = populateDataFromFrameworkTerm(frameworkId);
                    Map<String, Object> designationFrameworkObject = null;
                    Map<String, Object> competencyFrameworkObject = null;
                    if (CollectionUtils.isNotEmpty(getAllDesignationForOrg)) {
                        designationFrameworkObject = getAllDesignationForOrg.stream().filter(n -> ((String) (n.get("code")))
                                .equalsIgnoreCase(Constants.DESIGNATION)).findFirst().orElse(null);
                        competencyFrameworkObject = getAllDesignationForOrg.stream().filter(n -> ((String) (n.get("code")))
                                .equalsIgnoreCase(Constants.COMPETENCY)).findFirst().orElse(null);
                    }
                    String orgId = inputDataMap.get(Constants.ROOT_ORG_ID);
                    if (StringUtils.isNotEmpty(frameworkId)) {
                        orgDesignation = getOrgAddedDesignation(getAllDesignationForOrg);
                    }
                    Row nextRow = rowIterator.next();
                    boolean allColumnsEmpty = true;
                    for (int colIndex = 0; colIndex < 4; colIndex++) { // Only check the first 4 columns
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
                    long duration = 0;
                    long startTime = System.currentTimeMillis();
                    StringBuffer str = new StringBuffer();
                    List<String> errList = new ArrayList<>();
                    List<String> invalidErrList = new ArrayList<>();
                    Map<String, Object> competencyDesignationMappingInfoMap = new HashMap<>();
                    if (nextRow.getCell(0) == null || nextRow.getCell(0).getCellType() == CellType.BLANK) {
                        errList.add("Designation");
                    } else {
                        if (nextRow.getCell(0).getCellType() == CellType.STRING) {
                            String designation = nextRow.getCell(0).getStringCellValue().trim();
                            if (CollectionUtils.isNotEmpty(orgDesignation) && orgDesignation.contains(designation)) {
                                competencyDesignationMappingInfoMap.put(Constants.DESIGNATION, designation);
                            } else {
                                invalidErrList.add("Invalid designation for org: " + designation);
                            }
                        } else {
                            invalidErrList.add("Invalid column type for designation. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(1) == null || nextRow.getCell(1).getCellType() == CellType.BLANK) {
                        errList.add("Competency Area");
                    } else {
                        if (nextRow.getCell(1).getCellType() == CellType.STRING) {
                            competencyDesignationMappingInfoMap.put(Constants.COMPETENCY_AREA_NAME, nextRow.getCell(1).getStringCellValue().trim());
                        } else {
                            invalidErrList.add("Invalid column type for Competency Area. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(2) == null || nextRow.getCell(2).getCellType() == CellType.BLANK) {
                        errList.add("Competency Theme");
                    } else {
                        if (nextRow.getCell(2).getCellType() == CellType.STRING) {
                            competencyDesignationMappingInfoMap.put(Constants.COMPETENCY_THEME_NAME, nextRow.getCell(2).getStringCellValue().trim());
                        } else {
                            invalidErrList.add("Invalid column type for Competency Theme. Expecting string format");
                        }
                    }
                    if (nextRow.getCell(3) == null || nextRow.getCell(3).getCellType() == CellType.BLANK) {
                        errList.add("Competency Sub Theme");
                    } else {
                        if (nextRow.getCell(3).getCellType() == CellType.STRING) {
                            competencyDesignationMappingInfoMap.put(Constants.COMPETENCY_SUB_THEME_NAME, nextRow.getCell(3).getStringCellValue().trim());
                        } else {
                            invalidErrList.add("Invalid column type Competency Sub Theme. Expecting string format");
                        }
                    }

                    Map<String, Object> statusErrorMsgMap = validateCompetencyHierarchy(competencyDesignationMappingInfoMap, getAllCompetenciesMapping);
                    if (MapUtils.isNotEmpty(statusErrorMsgMap) && !(boolean) statusErrorMsgMap.get(Constants.STATUS)) {
                        invalidErrList.add((String) statusErrorMsgMap.get(Constants.ERROR_MESSAGE));
                    }
                    Map<String, Object> designationObject = null;

                    Cell statusCell = nextRow.getCell(4);
                    Cell errorDetails = nextRow.getCell(5);
                    if (statusCell == null) {
                        statusCell = nextRow.createCell(4);
                    }
                    if (errorDetails == null) {
                        errorDetails = nextRow.createCell(5);
                    }
                    if (totalRecordsCount == 0 && errList.size() == 1) {
                        setErrorDetails(str, errList, statusCell, errorDetails);
                        failedRecordsCount++;
                    }
                    totalRecordsCount++;
                    progressUpdateThresholdValue++;
                    if (!errList.isEmpty()) {
                        setErrorDetails(str, errList, statusCell, errorDetails);
                        failedRecordsCount++;
                    } else {
                        if (MapUtils.isNotEmpty(designationFrameworkObject) && CollectionUtils.isEmpty(invalidErrList)) {
                            List<Map<String, Object>> designationTerms = (List<Map<String, Object>>) designationFrameworkObject.get(Constants.TERMS);
                            if (CollectionUtils.isNotEmpty(designationTerms)) {
                                designationObject = designationTerms.stream().filter(n -> ((String) n.get(Constants.NAME))
                                                .equalsIgnoreCase((String) competencyDesignationMappingInfoMap.get(Constants.DESIGNATION)))
                                        .findFirst().orElse(null);
                                if (MapUtils.isNotEmpty(designationObject)) {
                                    List<Map<String, Object>> designationAssociation = (List<Map<String, Object>>) designationObject.get(Constants.ASSOCIATIONS);
                                    if (CollectionUtils.isNotEmpty(designationAssociation)) {
                                        Map<String, Object> competencyThemeObject = designationAssociation.stream().filter(n -> ((String) n.get(Constants.NAME))
                                                        .equalsIgnoreCase((String) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_THEME_NAME)))
                                                .findFirst().orElse(null);
                                        if (MapUtils.isNotEmpty(competencyThemeObject)) {
                                            Map<String, Object> additionalProperties = (Map<String, Object>) competencyThemeObject.get(Constants.ADDITIONAL_PROPERTIES);
                                            if (MapUtils.isNotEmpty(additionalProperties)) {
                                                Map<String, Object> competencyArea = (Map<String, Object>) additionalProperties.get(Constants.COMPETENCY_AREA);
                                                boolean isCompetencyPresent = false;
                                                if (MapUtils.isNotEmpty(competencyArea)) {
                                                    isCompetencyPresent = ((String) competencyArea.get(Constants.NAME)).equalsIgnoreCase((String) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_AREA_NAME));
                                                }
                                                if (isCompetencyPresent) {
                                                    List<Map<String, Object>> competencyTerms = (List<Map<String, Object>>) competencyFrameworkObject.get(Constants.TERMS);
                                                    if (CollectionUtils.isNotEmpty(competencyTerms)) {
                                                        Map<String, Object> competencyObjectWithAssociations = competencyTerms.stream().filter(n -> ((String) n.get(Constants.IDENTIFIER))
                                                                        .equalsIgnoreCase((String) competencyThemeObject.get(Constants.IDENTIFIER)))
                                                                .findFirst().orElse(null);
                                                        if (MapUtils.isNotEmpty(competencyObjectWithAssociations)) {
                                                            List<Map<String, Object>> competencyAssociations = (List<Map<String, Object>>) competencyObjectWithAssociations.get(Constants.ASSOCIATIONS);
                                                            if (CollectionUtils.isNotEmpty(competencyAssociations)) {
                                                                Map<String, Object> competencySubTheme = competencyAssociations.stream().filter(n -> ((String) n.get(Constants.NAME))
                                                                                .equalsIgnoreCase((String) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_SUB_THEME_NAME)))
                                                                        .findFirst().orElse(null);
                                                                if (MapUtils.isEmpty(competencySubTheme)) {
                                                                    // SubTheme is not present in association
                                                                    addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, false, competencyObjectWithAssociations, orgId);
                                                                } else {
                                                                    invalidErrList.add("Already the competency Associated with the designation.");
                                                                }
                                                            } else {
                                                                // No Association Present is competency Theme
                                                                addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, false, competencyThemeObject, orgId);
                                                            }
                                                        } else {
                                                            // No competency Object is present
                                                            addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, true, null, orgId);
                                                        }
                                                    } else {
                                                        // No term object is present in competency Object
                                                        addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, true, null, orgId);
                                                    }
                                                } else {
                                                    // competency Theme Object is present with different Competency Area
                                                    addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, true, null, orgId);
                                                }
                                            } else {
                                                addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, true, null, orgId);
                                            }
                                        } else {
                                            // Theme object need to created
                                            addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, true, null, orgId);
                                        }
                                    } else {
                                        // Theme object need to created
                                        addUpdateDesignationCompetencyMapping(frameworkId, competencyDesignationMappingInfoMap, designationObject, invalidErrList, true, null, orgId);
                                    }
                                } else {
                                    invalidErrList.add("The designation is not added in the framework.");
                                }
                            } else {
                                invalidErrList.add("The issue while fetching the framework for the org.");
                            }
                        } else {
                            if (CollectionUtils.isEmpty(invalidErrList)) {
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
                    Cell statusCell = row.createCell(4);
                    Cell errorDetails = row.createCell(5);
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

    private String getCodeValue(String value) {
        String regex = "[^_]+$";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(value);
        String nodeId = null;
        // Find the match
        if (matcher.find()) {
            nodeId = matcher.group();
        }
        return nodeId;
    }

    private Map<String, Object> validateCompetencyHierarchy(Map<String, Object> competencyDesignationMappingInfoMap, List<Map<String, Object>> getAllCompetenciesMapping) {
        Map<String, Object> statusErrorMsgMap = new HashMap<>();
        if (CollectionUtils.isNotEmpty(getAllCompetenciesMapping)) {
            String competencyArea = (String) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_AREA_NAME);
            String competencyTheme = (String) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_THEME_NAME);
            String competencySubTheme = (String) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_SUB_THEME_NAME);
            Map<String, Object> competencyAreaFrameworkObject = getAllCompetenciesMapping.stream().filter(n -> ((String) (n.get("code")))
                    .equalsIgnoreCase(Constants.COMPETENCY_AREA_LOWERCASE)).findFirst().orElse(null);
            Map<String, Object> competencyThemeFrameworkObject = getAllCompetenciesMapping.stream().filter(n -> ((String) (n.get("code")))
                    .equalsIgnoreCase(Constants.THEME)).findFirst().orElse(null);
            if (MapUtils.isNotEmpty(competencyAreaFrameworkObject) && MapUtils.isNotEmpty(competencyThemeFrameworkObject)) {
                List<Map<String, Object>> competencyAreaTerms = (List<Map<String, Object>>) competencyAreaFrameworkObject.get("terms");
                List<Map<String, Object>> competencyThemeTerms = (List<Map<String, Object>>) competencyThemeFrameworkObject.get("terms");
                if (CollectionUtils.isNotEmpty(competencyAreaTerms) && CollectionUtils.isNotEmpty(competencyThemeTerms)) {
                    Map<String, Object> competencyAreaObject = competencyAreaTerms.stream().filter(n -> ((String) n.get(Constants.NAME)).equalsIgnoreCase(competencyArea)).findFirst().map(HashMap::new).orElse(null);
                    if (MapUtils.isNotEmpty(competencyAreaObject)) {
                        competencyDesignationMappingInfoMap.put(Constants.COMPETENCY_AREA, competencyAreaObject);
                        List<Map<String, Object>> competencyAreaAssociations = (List<Map<String, Object>>) competencyAreaObject.get("associations");
                        if (CollectionUtils.isNotEmpty(competencyAreaAssociations)) {
                            Map<String, Object> competencyThemeObject = competencyAreaAssociations.stream().filter(n -> ((String) n.get(Constants.NAME)).equalsIgnoreCase(competencyTheme)).findFirst().map(HashMap::new).orElse(null);
                            if (MapUtils.isNotEmpty(competencyThemeObject)) {
                                competencyDesignationMappingInfoMap.put(Constants.SEARCH_COMPETENCY_THEMES, competencyThemeObject);
                                String identifier = (String) competencyThemeObject.get(Constants.IDENTIFIER);
                                Map<String, Object> themeObject = competencyThemeTerms.stream().filter(theme -> ((String) theme.get(Constants.IDENTIFIER)).equalsIgnoreCase(identifier))
                                        .findFirst().orElse(null);
                                if (MapUtils.isNotEmpty(themeObject)) {
                                    List<Map<String, Object>> themeObjectAssociations = (List<Map<String, Object>>) themeObject.get("associations");
                                    if (CollectionUtils.isNotEmpty(themeObjectAssociations)) {
                                        Map<String, Object> subThemeObject = themeObjectAssociations.stream().filter(n -> ((String) n.get(Constants.NAME)).equalsIgnoreCase(competencySubTheme)).findFirst().map(HashMap::new).orElse(null);
                                        if (MapUtils.isNotEmpty(subThemeObject)) {
                                            competencyDesignationMappingInfoMap.put(Constants.SEARCH_COMPETENCY_SUB_THEMES, subThemeObject);
                                            statusErrorMsgMap.put(Constants.STATUS, true);
                                            return statusErrorMsgMap;
                                        } else {
                                            statusErrorMsgMap.put(Constants.STATUS, false);
                                            statusErrorMsgMap.put(Constants.ERROR_MESSAGE, "The subTheme is not proper: " + competencySubTheme);
                                        }
                                    }
                                }
                            } else {
                                statusErrorMsgMap.put(Constants.STATUS, false);
                                statusErrorMsgMap.put(Constants.ERROR_MESSAGE, "The theme is not proper: " + competencyTheme);
                            }
                        } else {
                            statusErrorMsgMap.put(Constants.STATUS, false);
                            statusErrorMsgMap.put(Constants.ERROR_MESSAGE, "The theme associations not proper: " + competencyTheme);
                        }
                    } else {
                        statusErrorMsgMap.put(Constants.STATUS, false);
                        statusErrorMsgMap.put(Constants.ERROR_MESSAGE, "The competency Area is not proper: " + competencyArea);
                    }
                }
            }
        }
        return statusErrorMsgMap;
    }

    private void addUpdateDesignationCompetencyMapping(String frameworkId, Map<String, Object> competencyDesignationMappingInfoMap, Map<String, Object> designationObject,
                                                       List<String> invalidErrList, boolean isCompetencyNeedToBeAdd, Map<String, Object> competencyTheme, String orgId) {
        try {
            List<String> nodeIdsForSubTheme = null;
            String nodeIdForTheme = null;
            if (isCompetencyNeedToBeAdd) {
                Map<String, Object> termCreateResp = createUpdateTermFrameworkForCompetencyTheme(frameworkId, competencyDesignationMappingInfoMap);
                if (MapUtils.isNotEmpty(termCreateResp)) {
                    List<String> nodeIds = (List<String>) termCreateResp.get("node_id");
                    if (CollectionUtils.isNotEmpty(nodeIds)) {
                        List<Map<String, Object>> previousAssociations = (List<Map<String, Object>>) designationObject.get(Constants.ASSOCIATIONS);
                        if (CollectionUtils.isNotEmpty(previousAssociations)) {
                            nodeIds.addAll(previousAssociations.stream().map(n -> (String) n.get(Constants.IDENTIFIER)).collect(Collectors.toList()));
                        }
                        String nodeIdForDesignation = (String) designationObject.get(Constants.CODE);
                        List<Map<String, Object>> createObj = new ArrayList<>();
                        for (String nodeId : nodeIds) {
                            Map<String, Object> nodeIdMap = new HashMap<>();
                            nodeIdMap.put(Constants.IDENTIFIER, nodeId);
                            createObj.add(nodeIdMap);
                        }

                        Map<String, Object> frameworkAssociationUpdate = updateFrameworkTerm(frameworkId, updateRequestObject(createObj), Constants.DESIGNATION, nodeIdForDesignation);
                        if (MapUtils.isNotEmpty(frameworkAssociationUpdate)) {
                            nodeIdForTheme = getCodeValue(nodeIds.get(0));
                        } else {
                            invalidErrList.add("Issue while adding the subTheme to the framework for competency theme.");
                        }
                    } else {
                        invalidErrList.add("Issue while adding the associations to the framework for designation.");
                    }

                }
            }
            if (StringUtils.isEmpty(nodeIdForTheme) && MapUtils.isNotEmpty(competencyTheme)) {
                nodeIdForTheme = (String) competencyTheme.get(Constants.CODE);
            }
            Map<String, Object> termCreateRespCompetencySubTheme = createUpdateTermFrameworkForCompetencySubTheme(frameworkId, competencyDesignationMappingInfoMap);
            if (MapUtils.isNotEmpty(termCreateRespCompetencySubTheme)) {
                nodeIdsForSubTheme = (List<String>) termCreateRespCompetencySubTheme.get("node_id");
                if (MapUtils.isNotEmpty(competencyTheme)) {
                    List<Map<String, Object>> previousAssociationsTheme = (List<Map<String, Object>>) competencyTheme.get(Constants.ASSOCIATIONS);
                    if (CollectionUtils.isNotEmpty(previousAssociationsTheme)) {
                        nodeIdsForSubTheme.addAll(previousAssociationsTheme.stream().map(n -> (String) n.get(Constants.IDENTIFIER)).collect(Collectors.toList()));
                    }
                }
                if (CollectionUtils.isNotEmpty(nodeIdsForSubTheme)) {
                    List<Map<String, Object>> createSubThemeObject = new ArrayList<>();
                    for (String nodeId : nodeIdsForSubTheme) {
                        Map<String, Object> nodeIdMap = new HashMap<>();
                        nodeIdMap.put(Constants.IDENTIFIER, nodeId);
                        createSubThemeObject.add(nodeIdMap);
                    }
                    Map<String, Object> frameworkAssociationUpdateForCompetencyTheme = updateFrameworkTerm(frameworkId, updateRequestObject(createSubThemeObject), Constants.COMPETENCY, nodeIdForTheme);
                    if (MapUtils.isNotEmpty(frameworkAssociationUpdateForCompetencyTheme)) {
                        Map<String, Object> result = publishFramework(frameworkId, new HashMap<>(), orgId);
                        if (MapUtils.isNotEmpty(result)) {
                            logger.info("Publish is Success for frameworkId: " + frameworkId);
                        } else {
                            invalidErrList.add("Issue while publish the framework.");
                        }
                    } else {
                        invalidErrList.add("Issue while adding the associations to the framework for theme.");
                    }
                } else {
                    invalidErrList.add("Issue while adding the subTheme to the framework for competency theme.");
                }
            } else {
                invalidErrList.add("Issue while adding the subTheme to the framework for competency theme.");
            }
        } catch (Exception e) {
            logger.error("Issue while adding the subTheme to the framework for competency theme.", e);
            invalidErrList.add("Issue while adding the subTheme to the framework for competency theme.");
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
        SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getCompetencyDesignationBulkUploadContainerName(), serverProperties.getCloudContainerName());
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

    private Map<String, Object> createUpdateTermFrameworkForCompetencyTheme(String frameworkId, Map<String, Object> competencyDesignationMappingInfoMap) throws Exception {
        Map<String, Object> competencyThemeTermObject = competencyThemeTermObject(frameworkId, competencyDesignationMappingInfoMap);
        return createFrameworkTerm(frameworkId, competencyThemeTermObject, Constants.COMPETENCY);
    }

    private Map<String, Object> createUpdateTermFrameworkForCompetencySubTheme(String frameworkId, Map<String, Object> competencyDesignationMappingInfoMap) throws Exception {
        Map<String, Object> competencySubThemeTermObject = competencySubThemeTermObject(frameworkId, competencyDesignationMappingInfoMap);
        return createFrameworkTerm(frameworkId, competencySubThemeTermObject, Constants.SUBTHEME);
    }

    private Map<String, Object> competencyThemeTermObject(String frameworkId, Map<String, Object> competencyDesignationMappingInfoMap) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.CODE, UUIDs.timeBased());
        termReq.put(Constants.CATEGORY, Constants.COMPETENCY);
        Map<String, Object> competencyThemeMap = (Map<String, Object>) competencyDesignationMappingInfoMap.get(Constants.SEARCH_COMPETENCY_THEMES);
        competencyThemeMap.remove(Constants.IDENTIFIER);
        competencyThemeMap.remove(Constants.CODE);
        competencyThemeMap.remove(Constants.INDEX);
        competencyThemeMap.remove(Constants.CATEGORY);
        Map<String, Object> competencyAreaMap = new HashMap<>();
        Map<String, Object> competencyArea = (Map<String, Object>) competencyDesignationMappingInfoMap.get(Constants.COMPETENCY_AREA);
        competencyArea.remove(Constants.ASSOCIATIONS);
        competencyAreaMap.put(Constants.COMPETENCY_AREA, competencyArea);
        competencyAreaMap.put(Constants.TIMESTAMP, Instant.now().toEpochMilli());
        competencyThemeMap.put(Constants.ADDITIONAL_PROPERTIES, competencyAreaMap);
        termReq.putAll(competencyThemeMap);

        Map<String, Object> parentObj = new HashMap<>();
        parentObj.put(Constants.IDENTIFIER, frameworkId + "_" + Constants.COMPETENCY);
        termReq.put(Constants.PARENTS, Arrays.asList(parentObj));
        requestBody.put(Constants.TERM, termReq);
        Map<String, Object> createReq = new HashMap<>();
        createReq.put(Constants.REQUEST, requestBody);
        return createReq;
    }

    private Map<String, Object> competencySubThemeTermObject(String frameworkId, Map<String, Object> competencyDesignationMappingInfoMap) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.CODE, UUIDs.timeBased());
        termReq.put(Constants.CATEGORY, Constants.SUBTHEME);
        Map<String, Object> competencySubThemeMap = (Map<String, Object>) competencyDesignationMappingInfoMap.get(Constants.SEARCH_COMPETENCY_SUB_THEMES);
        competencySubThemeMap.remove(Constants.IDENTIFIER);
        competencySubThemeMap.remove(Constants.CODE);
        competencySubThemeMap.remove(Constants.INDEX);
        competencySubThemeMap.remove(Constants.CATEGORY);
        Map<String, Object> competencyMap = new HashMap<>();
        competencyMap.put(Constants.TIMESTAMP, Instant.now().toEpochMilli());
        competencySubThemeMap.put(Constants.ADDITIONAL_PROPERTIES, competencyMap);
        termReq.putAll(competencySubThemeMap);

        Map<String, Object> parentObj = new HashMap<>();
        parentObj.put(Constants.IDENTIFIER, frameworkId + "_" + Constants.SUBTHEME);
        termReq.put(Constants.PARENTS, Arrays.asList(parentObj));
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

    private List<Map<String, Object>> getMasterCompetencyFrameworkData(String frameworkId) throws IOException {
        String masterDataCompetencies = redisCacheMgr.getCache(Constants.COMPETENCY_MASTER_DATA + "_" + frameworkId);
        if (StringUtils.isEmpty(masterDataCompetencies)) {
            List<Map<String, Object>> competenciesMasterData = populateDataFromFrameworkTerm(serverProperties.getMasterCompetencyFrameworkName());
            redisCacheMgr.putCache(Constants.COMPETENCY_MASTER_DATA + "_" + frameworkId, competenciesMasterData, serverProperties.getRedisMasterDataReadTimeOut());
            return competenciesMasterData;
        } else {
            return objectMapper.readValue(masterDataCompetencies, new TypeReference<List<Map<String, Object>>>() {
            });
        }
    }
}
