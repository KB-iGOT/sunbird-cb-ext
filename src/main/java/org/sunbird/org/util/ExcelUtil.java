package org.sunbird.org.util;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.ss.util.CellReference;
import org.apache.poi.xssf.usermodel.XSSFDataValidationHelper;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.common.util.CbExtServerProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
@Service
public class ExcelUtil {


    @Autowired FrameworkUtil frameworkUtil;

    @Autowired
    CbExtServerProperties serverProperties;


    public void populateReferenceSheetCompetency(List<Map<String, Object>> categoryList, Sheet sheet) {
        int[] rowIndex = {1};
        if (categoryList != null && !categoryList.isEmpty()) {
            List<Map<String, Object>> l1Terms = (List<Map<String, Object>>) categoryList.get(0).get("terms");
            if (l1Terms != null) {
                for (Map<String, Object> term : l1Terms) {
                    frameworkUtil.traverseByCategory(sheet, categoryList, 0, term, new ArrayList<>(), rowIndex);
                }
            }
        }
    }

    public void createHeaderRowForYourWorkBook(Sheet sheet, String[] headers) {
        Row headerRow = sheet.createRow(0);

        CellStyle headerStyle = sheet.getWorkbook().createCellStyle();
        Font font = sheet.getWorkbook().createFont();
        font.setBold(true);  // Make the header font bold
        headerStyle.setFont(font);
        headerStyle.setLocked(true);  // Lock the header cells

        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);

            sheet.autoSizeColumn(i);

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

    public static void createHeaderRow(Sheet sheet, String[] headers) {
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
        }
        sheet.createFreezePane(0, 1);
    }

    public void makeSheetReadOnly(Sheet sheet) {
        sheet.protectSheet("password");
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

    private void setDropdownForColumn(
            XSSFDataValidationHelper validationHelper,
            Sheet targetSheet,
            int targetColumn,
            String formulaRange
    ) {
        int firstRow = 1;
        int lastRow = 1000;

        DataValidationConstraint constraint = validationHelper.createFormulaListConstraint(formulaRange);
        CellRangeAddressList addressList = new CellRangeAddressList(firstRow, lastRow, targetColumn, targetColumn);
        DataValidation validation = validationHelper.createValidation(constraint, addressList);


        validation.createPromptBox("Note", serverProperties.getValidationMessageDuplicateLevels());
        validation.setShowPromptBox(true);

        validation.setShowErrorBox(true);

        targetSheet.addValidationData(validation);
    }

    public void setColumnWidths(Sheet sheet) {
        for (int i = 0; i < sheet.getRow(0).getPhysicalNumberOfCells(); i++) {
            sheet.setColumnWidth(i, serverProperties.getOrgHierarchyColumnWidth());
        }
    }

    public void addDuplicateHighlighting(Sheet sheet, int startRow, int endRow, int startCol, int endCol) {
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

    public void addSameOrgHighlighting(Sheet sheet, int startRow, int endRow, int validationCol, int currentOrgCol, int targetOrgCol) {
        SheetConditionalFormatting sheetCF = sheet.getSheetConditionalFormatting();
        String validationColLetter = CellReference.convertNumToColString(validationCol);

        String formula = validationColLetter + (startRow + 1) + "=\"SAME_ORG\"";

        ConditionalFormattingRule rule = sheetCF.createConditionalFormattingRule(formula);

        PatternFormatting fill = rule.createPatternFormatting();
        fill.setFillBackgroundColor(IndexedColors.PINK.getIndex());
        fill.setFillPattern(PatternFormatting.SOLID_FOREGROUND);

        CellRangeAddress[] regions = {
                new CellRangeAddress(startRow, endRow, currentOrgCol, targetOrgCol)
        };

        sheetCF.addConditionalFormatting(regions, rule);
    }

}
