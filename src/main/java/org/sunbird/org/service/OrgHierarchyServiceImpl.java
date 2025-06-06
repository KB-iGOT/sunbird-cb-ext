package org.sunbird.org.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.ss.util.WorkbookUtil;
import org.apache.poi.xssf.usermodel.XSSFDataValidation;
import org.apache.poi.xssf.usermodel.XSSFDataValidationHelper;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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


    @Override
    public ResponseEntity<ByteArrayResource> bulkUploadOrganisationMapping(String rootOrgId, String userAuthToken, String frameworkId) {
        try {
            Workbook workbook = new XSSFWorkbook();

            Sheet referenceSheetCompetency = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyReferenceWorkSpaceName()));
            Sheet orgDesignationMasterSheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(serverProperties.getBulkUploadOrgHierarchyMasterDesignationWorkSpaceName()));
            List<Map<String, Object>> categoryList = populateDataFromFrameworkTerm(frameworkId);
            String[] referenceSheetHeaders = categoryList.stream()
                    .map(cat -> (String) cat.get(Constants.CODE))
                    .filter(code -> code != null)
                    .toArray(String[]::new);
            createHeaderRowForYourWorkBook(referenceSheetCompetency, referenceSheetHeaders);
            createHeaderRow(orgDesignationMasterSheet, serverProperties.getBulkUploadOrgHierarchyMasterDataHeaders());

            populateOrgDesignationMaster(orgDesignationMasterSheet);

            makeSheetReadOnly(orgDesignationMasterSheet);

            orgDesignationMasterSheet.setColumnHidden(2, true);

            setUpDropdowns(workbook, referenceSheetCompetency, orgDesignationMasterSheet, serverProperties.getBulkUploadOrgHierarchyReferencesHeaders());

            setColumnWidths(referenceSheetCompetency);

            setColumnWidths(orgDesignationMasterSheet);

            addDuplicateHighlighting(referenceSheetCompetency, 1, 1000, 0, serverProperties.getBulkUploadOrgHierarchyReferencesHeaders().length - 1);

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
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }


    private static void createHeaderRow(Sheet sheet, String[] headers) {
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
        }
        sheet.createFreezePane(0, 1);
    }


    public void setUpDropdowns(
            Workbook workbook,
            Sheet referenceSheet,
            Sheet masterDataSheet,
            String[] headers
    ) {
        XSSFDataValidationHelper validationHelper = new XSSFDataValidationHelper((XSSFSheet) referenceSheet);

        int lastRow = masterDataSheet.getLastRowNum() + 1;
        String dropdownRange = "'" + masterDataSheet.getSheetName() + "'!$C$2:$C$" + lastRow;

        for (int i = 0; i < headers.length; i++) {
            setDropdownForColumn(validationHelper, referenceSheet, i, dropdownRange);
        }
    }


    private void populateOrgDesignationMaster(Sheet sheet) throws Exception {
        List<Map<String, Object>> orgList = getMasterData();
        int rowIndex = 1;

        for (Map<String, Object> org : orgList) {
            Row row = sheet.createRow(rowIndex++);

            String orgId = org.get("id") != null ? org.get("id").toString() : "";
            String orgName = org.get("orgName") != null ? org.get("orgName").toString() : "";

            row.createCell(0).setCellValue(orgId);

            row.createCell(1).setCellValue(orgName);

            row.createCell(2).setCellValue(orgName + " (" + orgId + ")");
        }
    }

    private List<Map<String, Object>> getMasterData() throws Exception, InterruptedException {
        String masterDataOrg = redisCacheMgr.getCache(Constants.ORG_MASTER_DATA);
        if (StringUtils.isEmpty(masterDataOrg)) {
            List<Map<String, Object>> orgMasterData = populateDataFromApi();
            redisCacheMgr.putCache(Constants.ORG_MASTER_DATA, orgMasterData, serverProperties.getRedisMasterDataReadTimeOut());
            return orgMasterData;
        } else {
            return objectMapper.readValue(masterDataOrg, new TypeReference<List<Map<String, Object>>>() {
            });
        }
    }

    private List<Map<String, Object>> populateDataFromApi() throws Exception {
        Thread.sleep(500);
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
        String url = serverProperties.getLearnerServiceHost() + serverProperties.getOrgSearchUrl();
        Map<String, Object> termFrameworkCompetencies = (Map<String, Object>) outboundRequestHandler.fetchResultUsingPost(
                url, buildOrgSearchRequest(),headers);
        if (MapUtils.isNotEmpty(termFrameworkCompetencies)) {
            Map<String, Object> result = ((Map<String, Object>) termFrameworkCompetencies.get(Constants.RESULT));
            if (MapUtils.isNotEmpty(result)) {
                Map<String, Object> frameworkObject = ((Map<String, Object>) result.get(Constants.RESPONSE));
                if (MapUtils.isNotEmpty(frameworkObject)) {
                    return (List<Map<String, Object>>) frameworkObject.get(Constants.CONTENT);
                }
            }
        }
        return null;
    }

    private void setColumnWidths(Sheet sheet) {
        for (int i = 0; i < sheet.getRow(0).getPhysicalNumberOfCells(); i++) {
            sheet.setColumnWidth(i, 8000);
        }
    }

    public Map<String, Object> buildOrgSearchRequest() {
        Map<String, Object> request = new HashMap<>();

        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.STATUS, 1);

        Map<String, String> sortBy = new HashMap<>();
        sortBy.put(Constants.ORG_NAME, Constants.ASC_ORDER);

        List<String> fields = Arrays.asList(Constants.ORG_NAME, Constants.ID);

        Map<String, Object> innerRequest = new HashMap<>();
        innerRequest.put(Constants.FILTERS, filters);
        innerRequest.put(Constants.SORT_BY, sortBy);
        innerRequest.put(Constants.FIELDS_CONSTANT, fields);
        innerRequest.put(Constants.LIMIT, serverProperties.getOrgSearchLimit());
        innerRequest.put(Constants.OFFSET, 0);

        request.put(Constants.REQUEST, innerRequest);

        return request;
    }

    private void setDropdownForColumn(
            XSSFDataValidationHelper validationHelper,
            Sheet targetSheet,
            int targetColumn,
            String formulaRange
    ) {
        int firstRow = 1;  // Start from row 2
        int lastRow = 1000; // Adjust if needed

        // 1. Create dropdown list using data validation
        DataValidationConstraint constraint = validationHelper.createFormulaListConstraint(formulaRange);
        CellRangeAddressList addressList = new CellRangeAddressList(firstRow, lastRow, targetColumn, targetColumn);
        DataValidation validation = validationHelper.createValidation(constraint, addressList);

        // 2. Tooltip message (Prompt box)
        validation.createPromptBox("Note", "Avoid selecting duplicate values across levels (L1 to L10).");
        validation.setShowPromptBox(true);

        // Optional error box on invalid input
        validation.setShowErrorBox(true);

        // 3. Apply the validation
        targetSheet.addValidationData(validation);
    }


    private void makeSheetReadOnly(Sheet sheet) {
        sheet.protectSheet("password");
    }

    private void addDuplicateHighlighting(Sheet sheet, int startRow, int endRow, int startCol, int endCol) {
        SheetConditionalFormatting sheetCF = sheet.getSheetConditionalFormatting();

        // Convert start and end column indices to letters, e.g. 0 -> A, 9 -> J
        String startColLetter = CellReference.convertNumToColString(startCol);
        String endColLetter = CellReference.convertNumToColString(endCol);

        // The conditional formatting formula to check duplicates within the row only
        // Example: =COUNTIF($A1:$J1, A1) > 1
        // The $ locks columns, row is relative, so it works for each row and cell
        String formula = "COUNTIF($" + startColLetter + (startRow + 1) + ":$" + endColLetter + (startRow + 1) + "," +
                CellReference.convertNumToColString(startCol) + (startRow + 1) + ") > 1";

        // Create conditional formatting rule with the formula
        ConditionalFormattingRule rule = sheetCF.createConditionalFormattingRule(formula);

        // Set the fill color to Rose (pink)
        PatternFormatting fill = rule.createPatternFormatting();
        fill.setFillBackgroundColor(IndexedColors.ROSE.getIndex());
        fill.setFillPattern(PatternFormatting.SOLID_FOREGROUND);

        // Define the range where this conditional formatting will apply
        CellRangeAddress[] regions = {
                new CellRangeAddress(startRow, endRow, startCol, endCol)
        };

        // Add the conditional formatting to the sheet
        sheetCF.addConditionalFormatting(regions, rule);
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

    private List<Map<String, Object>> populateDataFromFrameworkTerm(String frameworkId) throws Exception {
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

}
