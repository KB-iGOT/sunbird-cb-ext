package org.sunbird.nongovtuser.service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Function;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.nongovtuser.model.RowProcessingSummary;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.storage.service.StorageService;

/**
 * Async processing side of Non-Govt (Volunteer) bulk user upload.
 * Mirrors UserBulkUploadService.processCSVBulkUploadV2 / processBulkUpload for the
 * simplified 4-field volunteer schema (Full Name, Mobile Number, Email, External ID),
 * supporting both CSV and XLSX like the reference — but, unlike that reference,
 * doesn't abort the whole file on a blank row, and adds duplicate-mobile detection
 * and a PARTIALLY-COMPLETED status.
 * <p>
 * Row extraction is format-specific (CSV via commons-csv, XLSX via POI), but every row
 * is normalized to the same field map before validation/user-creation, so that logic
 * is shared between both formats rather than duplicated.
 */
@RequiredArgsConstructor
@Service
public class NonGovtUserBulkUploadProcessingServiceImpl implements NonGovtUserBulkUploadProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(NonGovtUserBulkUploadProcessingServiceImpl.class);

    private static final List<String> RESULT_HEADERS = Arrays.asList(
            Constants.NON_GOVT_CSV_COLUMN_FULL_NAME,
            Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER,
            Constants.NON_GOVT_CSV_COLUMN_EMAIL,
            Constants.NON_GOVT_CSV_COLUMN_EXTERNAL_ID,
            Constants.PASCALCASESTATUS,
            Constants.CSV_COLUMN_ERROR_DETAILS);

    private static final DataFormatter EXCEL_CELL_FORMATTER = new DataFormatter();

    private final ObjectMapper objectMapper;
    private final CassandraOperation cassandraOperation;
    private final StorageService storageService;
    private final CbExtServerProperties serverProperties;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Override
    public void initiateNonGovtUserBulkUploadProcess(String inputData) {
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: initiateNonGovtUserBulkUploadProcess: Started");
        long startTime = System.currentTimeMillis();
        String identifier = null;
        try {
            Map<String, String> inputDataMap = deserializeKafkaMessage(inputData);
            if (MapUtils.isEmpty(inputDataMap)) {
                return;
            }
            identifier = inputDataMap.get(Constants.IDENTIFIER);
            List<String> validationErrors = validateReceivedMessage(inputDataMap);
            if (!validationErrors.isEmpty()) {
                logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: initiateNonGovtUserBulkUploadProcess: "
                        + "Invalid Kafka message for identifier: {}, errors: {}", identifier, validationErrors);
                return;
            }
            markUploadInProgress(inputDataMap);
            processBulkUpload(inputDataMap);
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: initiateNonGovtUserBulkUploadProcess: Failed", e);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: initiateNonGovtUserBulkUploadProcess: "
                    + "Completed for identifier: {}. Time taken: {} ms", identifier, duration);
        }
    }

    /**
     * Parses the Kafka payload into a flat string map. Returns an empty map (and logs)
     * on malformed JSON rather than throwing, so the caller can bail out cleanly.
     */
    private Map<String, String> deserializeKafkaMessage(String inputData) {
        try {
            return objectMapper.readValue(inputData, new TypeReference<Map<String, String>>() {
            });
        } catch (IOException e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: deserializeKafkaMessage: Failed to parse Kafka message", e);
            return Collections.emptyMap();
        }
    }

    /**
     * Confirms every field NonGovtUserBulkUploadServiceImpl.triggerBulkUploadKafkaEvent
     * is expected to have published is actually present.
     */
    private List<String> validateReceivedMessage(Map<String, String> inputDataMap) {
        List<String> errors = new ArrayList<>();
        if (StringUtils.isBlank(inputDataMap.get(Constants.ROOT_ORG_ID))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.ROOT_ORG_ID));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.IDENTIFIER))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.IDENTIFIER));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.FILE_NAME))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.FILE_NAME));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.ORG_NAME))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.ORG_NAME));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.X_AUTH_TOKEN))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.X_AUTH_TOKEN));
        }
        return errors;
    }

    private void markUploadInProgress(Map<String, String> inputDataMap) {
        String identifier = inputDataMap.get(Constants.IDENTIFIER);
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: markUploadInProgress: identifier: {}", identifier);
        updateNonGovtUserBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID), identifier,
                Constants.STATUS_IN_PROGRESS_UPPERCASE, 0, 0, 0);
    }

    /**
     * Downloads the file, parses/validates/processes every row, writes and uploads the
     * annotated results file (same format as the upload), and persists the final job
     * outcome. Always leaves the job in a terminal status
     * (SUCCESSFUL/PARTIALLY-COMPLETED/FAILED) — never silently stalls, unlike the govt
     * reference for some of its failure branches.
     */
    private void processBulkUpload(Map<String, String> inputDataMap) {
        String rootOrgId = inputDataMap.get(Constants.ROOT_ORG_ID);
        String identifier = inputDataMap.get(Constants.IDENTIFIER);
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: processBulkUpload: Started for identifier: {}, rootOrgId: {}",
                identifier, rootOrgId);
        File file = downloadUploadedFile(inputDataMap.get(Constants.FILE_NAME));
        if (ObjectUtils.isEmpty(file)) {
            updateNonGovtUserBulkUploadStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
            return;
        }
        try {
            processDownloadedFile(file, rootOrgId, identifier, inputDataMap);
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: processBulkUpload: Failed for identifier: {}", identifier, e);
            updateNonGovtUserBulkUploadStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
        } finally {
            deleteLocalFile(file);
        }
    }

    private void deleteLocalFile(File file) {
        try {
            if (file != null && file.exists()) {
                Files.delete(file.toPath());
                logger.debug("NonGovtUserBulkUploadProcessingServiceImpl:: deleteLocalFile: Successfully deleted file: {}", file.getPath());
            } else {
                logger.debug("NonGovtUserBulkUploadProcessingServiceImpl:: deleteLocalFile: File does not exist or is null: {}",
                        file != null ? file.getPath() : "null");
            }
        } catch (IOException e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: deleteLocalFile: Failed to delete file: {}", file.getPath(), e);
        }
    }

    /**
     * Extracts rows in whichever format was uploaded (CSV or XLSX — same two formats
     * the govt reference supports), then runs the shared validation/creation pipeline
     * and writes results back out in that same format.
     */
    private void processDownloadedFile(File file, String rootOrgId, String identifier,
                                       Map<String, String> inputDataMap) throws IOException {
        String fileName = inputDataMap.get(Constants.FILE_NAME);
        String extension = getFileExtension(fileName);
        List<Map<String, String>> rawRows;
        if (Constants.CSV_FILE.equalsIgnoreCase(extension)) {
            rawRows = extractCsvRows(file);
        } else if (Constants.XLSX_FILE.equalsIgnoreCase(extension)) {
            rawRows = extractExcelRows(file);
        } else {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                            + "unsupported file type: {}",
                    identifier, fileName);
            updateNonGovtUserBulkUploadStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
            return;
        }
        if (CollectionUtils.isEmpty(rawRows)) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                    + "no rows found in uploaded file", identifier);
            updateNonGovtUserBulkUploadStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
            return;
        }
        if (rawRows.size() > serverProperties.getNonGovtBulkUploadMaxRows()) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                            + "row count {} exceeds the maximum allowed rows of {}",
                    identifier, rawRows.size(), serverProperties.getNonGovtBulkUploadMaxRows());
            updateNonGovtUserBulkUploadStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, rawRows.size(), 0, rawRows.size());
            return;
        }
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                + "extracted {} rows for processing", identifier, rawRows.size());
        RowProcessingSummary summary = processRows(rawRows, inputDataMap.get(Constants.ORG_NAME), inputDataMap.get(Constants.X_AUTH_TOKEN), rootOrgId);
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                        + "total: {}, successful: {}, failed: {}",
                identifier, summary.getTotalRecords(), summary.getSuccessfulRecords(), summary.getFailedRecords());
        String resultFileUrl = writeAndUploadResults(file, extension, summary.getUpdatedRecords());
        persistFinalOutcome(rootOrgId, identifier, summary, resultFileUrl);
    }

    private String getFileExtension(String fileName) {
        int lastIndexOfDot = fileName.lastIndexOf('.');
        return lastIndexOfDot == -1 ? StringUtils.EMPTY : fileName.substring(lastIndexOfDot);
    }

    private File downloadUploadedFile(String fileName) {
        storageService.downloadFile(fileName);
        File file = new File(Constants.LOCAL_BASE_PATH + fileName);
        if (!file.exists() || file.length() == 0) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: downloadUploadedFile: {}",
                    Constants.NON_GOVT_BULK_UPLOAD_FILE_NOT_FOUND_ERROR);
            return null;
        }
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: downloadUploadedFile: Downloaded {} ({} bytes)",
                fileName, file.length());
        return file;
    }

    private List<Map<String, String>> extractCsvRows(File file) {
        List<CSVRecord> records = parseCsvRecords(file);
        List<Map<String, String>> rows = new ArrayList<>();
        if (records.isEmpty()) {
            return rows;
        }
        Set<String> headers = records.get(0).toMap().keySet();
        String fullNameHeader = findMatchingHeader(headers,
                Constants.NON_GOVT_CSV_COLUMN_FULL_NAME, Constants.NON_GOVT_CSV_COLUMN_FULL_NAME_HEADER);
        String mobileNumberHeader = findMatchingHeader(headers,
                Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER, Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER_HEADER);
        for (CSVRecord csvRecord : records) {
            rows.add(extractRowFields(
                    getCsvFieldValue(csvRecord, fullNameHeader),
                    getCsvFieldValue(csvRecord, mobileNumberHeader),
                    getCsvFieldValue(csvRecord, Constants.NON_GOVT_CSV_COLUMN_EMAIL),
                    getCsvFieldValue(csvRecord, Constants.NON_GOVT_CSV_COLUMN_EXTERNAL_ID)));
        }
        return rows;
    }

    private List<CSVRecord> parseCsvRecords(File file) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8))) {
            char csvDelimiter = serverProperties.getCsvDelimiter();
            CSVFormat csvFormat = CSVFormat.newFormat(csvDelimiter).builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                return csvParser.getRecords();
            }
        } catch (IOException e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: parseCsvRecords: Failed to parse csv file", e);
            return Collections.emptyList();
        }
    }

    private String getCsvFieldValue(CSVRecord csvRecord, String columnName) {
        return (columnName != null && csvRecord.isSet(columnName)) ? csvRecord.get(columnName).trim() : StringUtils.EMPTY;
    }

    private List<Map<String, String>> extractExcelRows(File file) {
        try (InputStream fis = Files.newInputStream(file.toPath());
             Workbook workbook = new XSSFWorkbook(fis)) {
            Sheet sheet = workbook.getSheetAt(0);
            Map<String, Integer> columnIndexByHeader = readExcelHeaderRow(sheet);
            if (MapUtils.isEmpty(columnIndexByHeader)) {
                return Collections.emptyList();
            }
            String fullNameHeader = findMatchingHeader(columnIndexByHeader.keySet(),
                    Constants.NON_GOVT_CSV_COLUMN_FULL_NAME, Constants.NON_GOVT_CSV_COLUMN_FULL_NAME_HEADER);
            String mobileNumberHeader = findMatchingHeader(columnIndexByHeader.keySet(),
                    Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER, Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER_HEADER);
            List<Map<String, String>> rows = new ArrayList<>();
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (ObjectUtils.isEmpty(row)) {
                    continue;
                }
                rows.add(extractRowFields(
                        getExcelCellValue(row, columnIndexByHeader, fullNameHeader),
                        getExcelCellValue(row, columnIndexByHeader, mobileNumberHeader),
                        getExcelCellValue(row, columnIndexByHeader, Constants.NON_GOVT_CSV_COLUMN_EMAIL),
                        getExcelCellValue(row, columnIndexByHeader, Constants.NON_GOVT_CSV_COLUMN_EXTERNAL_ID)));
            }
            return rows;
        } catch (IOException e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: extractExcelRows: Failed to parse excel file", e);
            return Collections.emptyList();
        }
    }

    private Map<String, Integer> readExcelHeaderRow(Sheet sheet) {
        Row headerRow = sheet.getRow(0);
        if (ObjectUtils.isEmpty(headerRow)) {
            return Collections.emptyMap();
        }
        Map<String, Integer> columnIndexByHeader = new HashMap<>();
        for (Cell cell : headerRow) {
            String headerName = EXCEL_CELL_FORMATTER.formatCellValue(cell).trim();
            if (StringUtils.isNotBlank(headerName)) {
                columnIndexByHeader.put(headerName, cell.getColumnIndex());
            }
        }
        return columnIndexByHeader;
    }

    /**
     * Reads a cell's value as a plain string via DataFormatter rather than
     * Cell#setCellType/getStringCellValue — avoids mutating the cell and handles a
     * mobile number that Excel auto-typed as numeric without extra special-casing.
     */
    private String getExcelCellValue(Row row, Map<String, Integer> columnIndexByHeader, String columnName) {
        Integer columnIndex = columnName == null ? null : columnIndexByHeader.get(columnName);
        if (ObjectUtils.isEmpty(columnIndex)) {
            return StringUtils.EMPTY;
        }
        Cell cell = row.getCell(columnIndex);
        return ObjectUtils.isEmpty(cell) ? StringUtils.EMPTY : EXCEL_CELL_FORMATTER.formatCellValue(cell).trim();
    }

    /**
     * Finds the actual header text for a mandatory column, matching either its bare
     * name (e.g. "Full Name") or the "(mandatory)"-suffixed form the template header
     * row actually uses (e.g. "Full Name (mandatory)"). Returns null if neither is present.
     */
    private String findMatchingHeader(Set<String> headers, String bareColumnName, String columnNameWithMandatorySuffix) {
        for (String header : headers) {
            if (header.equalsIgnoreCase(bareColumnName) || header.equalsIgnoreCase(columnNameWithMandatorySuffix)) {
                return header;
            }
        }
        return null;
    }

    private Map<String, String> extractRowFields(String fullName, String mobileNumber, String email, String externalId) {
        Map<String, String> fields = new HashMap<>();
        fields.put(Constants.NON_GOVT_CSV_COLUMN_FULL_NAME, fullName);
        fields.put(Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER, mobileNumber);
        fields.put(Constants.NON_GOVT_CSV_COLUMN_EMAIL, email);
        fields.put(Constants.NON_GOVT_CSV_COLUMN_EXTERNAL_ID, externalId);
        return fields;
    }

    private RowProcessingSummary processRows(List<Map<String, String>> rawRows, String orgName, String userAuthToken, String rootOrgId) {
        RowProcessingSummary summary = new RowProcessingSummary();
        Set<String> seenMobileNumbers = new HashSet<>();
        int rowIndex = 1;
        for (Map<String, String> rawRow : rawRows) {
            summary.recordRow(processSingleRow(rawRow, rowIndex++, seenMobileNumbers, orgName, userAuthToken, rootOrgId));
        }
        return summary;
    }

    /**
     * Validates one row and, if valid, creates the volunteer user. Never throws —
     * any unexpected failure is caught and recorded against this row only, so one bad
     * row can't abort the rest of the file (matches "valid rows are processed
     * regardless of failures"). Failures are logged against the row's position
     * (rowIndex), never against the mobile number/email/name — those are PII and must
     * not appear in logs; the annotated results file is the only place per-row detail
     * is written.
     */
    private Map<String, String> processSingleRow(Map<String, String> rawRow, int rowIndex, Set<String> seenMobileNumbers,
                                                 String orgName, String userAuthToken, String orgId) {
        String fullName = rawRow.get(Constants.NON_GOVT_CSV_COLUMN_FULL_NAME);
        String mobileNumber = rawRow.get(Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER);
        String email = rawRow.get(Constants.NON_GOVT_CSV_COLUMN_EMAIL);
        String externalId = rawRow.get(Constants.NON_GOVT_CSV_COLUMN_EXTERNAL_ID);

        Map<String, String> updatedRecord = new HashMap<>(rawRow);
        try {
            List<String> rowErrors = validateRow(fullName, mobileNumber, email, externalId, seenMobileNumbers);
            if (!rowErrors.isEmpty()) {
                markRowFailed(updatedRecord, String.join(Constants.COMMA + " ", rowErrors));
                return updatedRecord;
            }
            seenMobileNumbers.add(mobileNumber);
            String creationError = createVolunteerUser(fullName, mobileNumber, email, externalId, orgName, userAuthToken, rowIndex, orgId);
            if (StringUtils.isNotBlank(creationError)) {
                markRowFailed(updatedRecord, creationError);
            } else {
                markRowSuccessful(updatedRecord);
            }
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: processSingleRow: Unexpected error for row: {}",
                    rowIndex, e);
            markRowFailed(updatedRecord, Constants.NON_GOVT_BULK_UPLOAD_ROW_PROCESSING_ERROR);
        }
        return updatedRecord;
    }

    /**
     * Mandatory: Full Name, Mobile Number. Optional: Email, External ID. Every field is
     * checked through the same FieldValidationRule pipeline (blank/mandatory check, then
     * pattern check) instead of one hand-written if/else-if per field, so adding a new
     * field later means adding one rule, not one more copy of the same branching.
     * Also flags mobile numbers already seen earlier in this same file as duplicates.
     */
    private List<String> validateRow(String fullName, String mobileNumber, String email, String externalId,
                                     Set<String> seenMobileNumbers) {
        List<String> errors = new ArrayList<>();
        if (StringUtils.isBlank(fullName) && StringUtils.isBlank(mobileNumber)
                && StringUtils.isBlank(email) && StringUtils.isBlank(externalId)) {
            errors.add(Constants.NON_GOVT_BULK_UPLOAD_EMPTY_ROW_ERROR);
            return errors;
        }

        List<FieldValidationRule> rules = Arrays.asList(
                new FieldValidationRule(Constants.NON_GOVT_CSV_COLUMN_FULL_NAME, fullName, true,
                        ProjectUtil::validateFullName, Constants.NON_GOVT_BULK_UPLOAD_INVALID_FULL_NAME_ERROR),
                new FieldValidationRule(Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER, mobileNumber, true,
                        ProjectUtil::validateContactPattern, Constants.NON_GOVT_BULK_UPLOAD_INVALID_MOBILE_ERROR),
                new FieldValidationRule(Constants.NON_GOVT_CSV_COLUMN_EMAIL, email, false,
                        ProjectUtil::validateEmailPattern, Constants.NON_GOVT_BULK_UPLOAD_INVALID_EMAIL_ERROR),
                new FieldValidationRule(Constants.NON_GOVT_CSV_COLUMN_EXTERNAL_ID, externalId, false,
                        ProjectUtil::validateExternalSystemId, Constants.NON_GOVT_BULK_UPLOAD_INVALID_EXTERNAL_ID_ERROR));

        for (FieldValidationRule rule : rules) {
            List<String> fieldErrors = applyFieldValidationRule(rule);
            errors.addAll(fieldErrors);
            if (fieldErrors.isEmpty() && Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER.equals(rule.columnName)
                    && seenMobileNumbers.contains(rule.value)) {
                errors.add(Constants.NON_GOVT_BULK_UPLOAD_DUPLICATE_MOBILE_ERROR);
            }
        }
        return errors;
    }

    private List<String> applyFieldValidationRule(FieldValidationRule rule) {
        List<String> errors = new ArrayList<>();
        if (StringUtils.isBlank(rule.value)) {
            if (rule.mandatory) {
                errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, rule.columnName));
            }
        } else if (!Boolean.TRUE.equals(rule.patternValidator.apply(rule.value))) {
            errors.add(rule.invalidPatternError);
        }
        return errors;
    }

    /**
     * One row's worth of field-validation configuration: which value to check, whether
     * it's mandatory, which ProjectUtil pattern check applies, and which constant to
     * raise if that pattern check fails. Kept as a private nested holder rather than a
     * new file since it's only ever used to drive applyFieldValidationRule above.
     */
    private static final class FieldValidationRule {
        private final String columnName;
        private final String value;
        private final boolean mandatory;
        private final Function<String, Boolean> patternValidator;
        private final String invalidPatternError;

        private FieldValidationRule(String columnName, String value, boolean mandatory,
                                    Function<String, Boolean> patternValidator, String invalidPatternError) {
            this.columnName = columnName;
            this.value = value;
            this.mandatory = mandatory;
            this.patternValidator = patternValidator;
            this.invalidPatternError = invalidPatternError;
        }
    }

    private void markRowFailed(Map<String, String> updatedRecord, String errorDetails) {
        updatedRecord.put(Constants.PASCALCASESTATUS, Constants.FAILED_UPPERCASE);
        updatedRecord.put(Constants.CSV_COLUMN_ERROR_DETAILS, errorDetails);
    }

    private void markRowSuccessful(Map<String, String> updatedRecord) {
        updatedRecord.put(Constants.PASCALCASESTATUS, Constants.SUCCESSFUL_UPPERCASE);
        updatedRecord.put(Constants.CSV_COLUMN_ERROR_DETAILS, StringUtils.EMPTY);
    }


    private String writeAndUploadResults(File file, String extension, List<Map<String, String>> updatedRecords) throws IOException {
        if (Constants.XLSX_FILE.equalsIgnoreCase(extension)) {
            writeResultExcel(file, updatedRecords);
        } else {
            writeResultCsv(file, updatedRecords);
        }
        return uploadResultFile(file);
    }

    private void writeResultCsv(File file, List<Map<String, String>> updatedRecords) throws IOException {
        char csvDelimiter = serverProperties.getCsvDelimiter();
        CSVFormat outputFormat = CSVFormat.newFormat(csvDelimiter).builder()
                .setHeader(RESULT_HEADERS.toArray(new String[0]))
                .setRecordSeparator(System.lineSeparator())
                .build();
        try (FileWriter fileWriter = new FileWriter(file);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
             CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, outputFormat)) {
            for (Map<String, String> rowData : updatedRecords) {
                List<String> row = new ArrayList<>();
                for (String header : RESULT_HEADERS) {
                    row.add(rowData.getOrDefault(header, StringUtils.EMPTY));
                }
                csvPrinter.printRecord(row);
            }
        }
    }

    private void writeResultExcel(File file, List<Map<String, String>> updatedRecords) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet();
            writeExcelHeaderRow(sheet);
            int rowIndex = 1;
            for (Map<String, String> rowData : updatedRecords) {
                writeExcelDataRow(sheet, rowIndex++, rowData);
            }
            try (FileOutputStream fos = new FileOutputStream(file)) {
                workbook.write(fos);
            }
        }
    }

    private void writeExcelHeaderRow(Sheet sheet) {
        Row headerRow = sheet.createRow(0);
        for (int i = 0; i < RESULT_HEADERS.size(); i++) {
            headerRow.createCell(i).setCellValue(RESULT_HEADERS.get(i));
        }
    }

    private void writeExcelDataRow(Sheet sheet, int rowIndex, Map<String, String> rowData) {
        Row row = sheet.createRow(rowIndex);
        for (int i = 0; i < RESULT_HEADERS.size(); i++) {
            row.createCell(i).setCellValue(rowData.getOrDefault(RESULT_HEADERS.get(i), StringUtils.EMPTY));
        }
    }

    private String uploadResultFile(File file) {
        SBApiResponse uploadResponse = storageService.uploadFile(
                file, serverProperties.getBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: uploadResultFile: {}",
                    Constants.NON_GOVT_BULK_UPLOAD_FILE_UPLOAD_ERROR);
            return null;
        }
        String resultFileUrl = (String) uploadResponse.getResult().get(Constants.URL);
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: uploadResultFile: Uploaded results file to {}",
                resultFileUrl);
        return resultFileUrl;
    }

    private void persistFinalOutcome(String rootOrgId, String identifier, RowProcessingSummary summary, String resultFileUrl) {
        String finalStatus = determineFinalStatus(summary);
        updateNonGovtUserBulkUploadStatus(rootOrgId, identifier, finalStatus,
                summary.getTotalRecords(), summary.getSuccessfulRecords(), summary.getFailedRecords());
        if (StringUtils.isNotBlank(resultFileUrl)) {
            updateResultFilePath(rootOrgId, identifier, resultFileUrl);
        }
        logger.info("NonGovtUserBulkUploadProcessingServiceImpl:: persistFinalOutcome: identifier: {}, "
                + "finalStatus: {}", identifier, finalStatus);
    }

    /**
     * Unlike the govt reference (binary SUCCESSFUL/FAILED only), this distinguishes a
     * partial outcome so callers can tell "some rows failed" from "everything failed".
     */
    private String determineFinalStatus(RowProcessingSummary summary) {
        if (summary.getTotalRecords() == 0 || summary.getSuccessfulRecords() == 0) {
            return Constants.FAILED_UPPERCASE;
        }
        if (summary.getFailedRecords() == 0) {
            return Constants.SUCCESSFUL_UPPERCASE;
        }
        return Constants.STATUS_PARTIALLY_COMPLETED_UPPERCASE;
    }

    /**
     * Persists the annotated results file's URL for both successful and failed uploads.
     * The results file contains the original data with added Status and Error Details columns.
     */
    private void updateResultFilePath(String rootOrgId, String identifier, String resultFileUrl) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);
            Map<String, Object> fieldsToBeUpdated = new HashMap<>();
            fieldsToBeUpdated.put(Constants.FILE_PATH_LOWER, resultFileUrl);
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_BULK_UPLOAD,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: updateResultFilePath: Failed for identifier: {}", identifier, e);
        }
    }

    /**
     * Updates the shared TABLE_USER_BULK_UPLOAD tracking row. Called from every
     * terminal branch of this class (mirrors UserBulkUploadService's
     * updateUserBulkUploadStatus).
     */
    private void updateNonGovtUserBulkUploadStatus(String rootOrgId, String identifier, String status,
                                                   int totalRecordsCount, int successfulRecordsCount,
                                                   int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);

            Map<String, Object> fieldsToBeUpdated = new HashMap<>();
            if (StringUtils.isNotBlank(status)) {
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

            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_BULK_UPLOAD,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: updateNonGovtUserBulkUploadStatus: "
                    + "Failed for identifier: {}", identifier, e);
        }
    }

    /**
     * Creates one volunteer user via the platform's LMS user create API. Deliberately
     * builds its own minimal request rather than reusing
     * UserUtilityServiceImpl.createBulkUploadUser (the govt equivalent), which
     * hardcodes organisationType=Government and carries cadre/civil-service fields
     * that don't apply here — kept to the 4-field volunteer schema for now.
     *
     * @return null on success, or a caller-facing error message on failure
     */
    private String createVolunteerUser(String fullName, String mobileNumber, String email, String externalId,
                                       String orgName, String userAuthToken, int rowIndex, String orgId) {
        try {
            Map<String, Object> request = buildVolunteerUserCreateRequest(fullName, mobileNumber, email, externalId, orgName);
            Map<String, String> headers = buildAuthHeaders(userAuthToken, orgId);
            Map<String, Object> createUserResponse = outboundRequestHandlerService.fetchResultUsingPost(
                    serverProperties.getSbUrl() + serverProperties.getLmsBulkUserCreatePath(), request, headers);
            String error = interpretCreateUserResponse(createUserResponse);
            if (StringUtils.isNotBlank(error)) {
                logger.warn("NonGovtUserBulkUploadProcessingServiceImpl:: createVolunteerUser: LMS returned failure for row: {}, reason: {}",
                        rowIndex, error);
            }
            return error;
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadProcessingServiceImpl:: createVolunteerUser: Failed for row: {}", rowIndex, e);
            return Constants.NON_GOVT_USER_CREATE_ERROR + " " + e.getMessage();
        }
    }

    private Map<String, String> buildAuthHeaders(String rootOrgId, String userAuthToken) {
        Map<String, String> headers = ProjectUtil.getDefaultHeaders();
        if (StringUtils.isNotBlank(userAuthToken)) {
            headers.put(Constants.X_AUTH_TOKEN, userAuthToken);
        }
        if (StringUtils.isNotBlank(rootOrgId)) {
            headers.put(Constants.X_AUTH_USER_ORG_ID, rootOrgId);
        }
        return headers;
    }

    private Map<String, Object> buildVolunteerUserCreateRequest(String fullName, String mobileNumber, String email,
                                                                String externalId, String orgName) {
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.FIRSTNAME, fullName);
        requestBody.put(Constants.PHONE, mobileNumber);
        requestBody.put(Constants.PHONE_VERIFIED, true);
        requestBody.put(Constants.CHANNEL, orgName);
        requestBody.put(Constants.ROLES, Collections.singletonList(serverProperties.getNonGovtUserDefaultRole()));
        if (StringUtils.isNotBlank(email)) {
            requestBody.put(Constants.EMAIL, email);
            requestBody.put(Constants.EMAIL_VERIFIED, true);
        }
        requestBody.put(Constants.PROFILE_DETAILS, buildVolunteerProfileDetails(fullName, mobileNumber, email, externalId));

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.REQUEST, requestBody);
        return request;
    }

    private Map<String, Object> buildVolunteerProfileDetails(String fullName, String mobileNumber, String email, String externalId) {
        Map<String, Object> profileDetails = new HashMap<>();
        profileDetails.put(Constants.MANDATORY_FIELDS_EXISTS, false);
        profileDetails.put(Constants.VERIFIED_KARMAYOGI, false);
        profileDetails.put(Constants.PROFILE_STATUS_UPDATED_ON, currentIstTimestamp());
        profileDetails.put(Constants.PERSONAL_DETAILS, buildVolunteerPersonalDetails(fullName, mobileNumber, email));
        profileDetails.put(Constants.PROFESSIONAL_DETAILS, buildVolunteerProfessionalDetails());
        Map<String, Object> additionalProperties = buildVolunteerAdditionalProperties(externalId);
        if (MapUtils.isNotEmpty(additionalProperties)) {
            profileDetails.put(Constants.ADDITIONAL_PROPERTIES, additionalProperties);
        }
        return profileDetails;
    }

    /**
     * Sets organisationType for the volunteer, configurable via
     * nongovt.user.organisation.type (default "Volunteer") — mirrors the shape of the
     * govt reference's professionalDetails[0].organisationType (hardcoded "Government"
     * there), but ours is configurable rather than a fixed Java constant.
     */
    private List<Map<String, Object>> buildVolunteerProfessionalDetails() {
        Map<String, Object> professionalDetail = new HashMap<>();
        professionalDetail.put(Constants.ORGANIZATION_TYPE, serverProperties.getNonGovtUserOrganisationType());
        return Collections.singletonList(professionalDetail);
    }

    private Map<String, Object> buildVolunteerPersonalDetails(String fullName, String mobileNumber, String email) {
        Map<String, Object> personalDetails = new HashMap<>();
        personalDetails.put(Constants.FIRSTNAME.toLowerCase(), fullName);
        personalDetails.put(Constants.MOBILE, mobileNumber);
        personalDetails.put(Constants.PHONE_VERIFIED, true);
        if (StringUtils.isNotBlank(email)) {
            personalDetails.put(Constants.PRIMARY_EMAIL, email);
        }
        return personalDetails;
    }

    private Map<String, Object> buildVolunteerAdditionalProperties(String externalId) {
        Map<String, Object> additionalProperties = new HashMap<>();
        if (StringUtils.isNotBlank(externalId)) {
            additionalProperties.put(Constants.EXTERNAL_SYSTEM_ID, externalId);
        }
        return additionalProperties;
    }

    private String currentIstTimestamp() {
        SimpleDateFormat sdf = new SimpleDateFormat(Constants.PROFILE_STATUS_DATE_FORMAT);
        sdf.setTimeZone(TimeZone.getTimeZone(Constants.IST_TIMEZONE));
        return sdf.format(new Date());
    }

    /**
     * Returns null on a successful create response, otherwise a caller-facing error
     * message extracted from the downstream response (or a generic fallback).
     */
    private String interpretCreateUserResponse(Map<String, Object> createUserResponse) {
        if (MapUtils.isEmpty(createUserResponse)) {
            return Constants.NON_GOVT_USER_CREATE_EMPTY_RESPONSE_ERROR;
        }
        if (Constants.OK.equalsIgnoreCase((String) createUserResponse.get(Constants.RESPONSE_CODE))) {
            return null;
        }
        Map<String, Object> params = (Map<String, Object>) createUserResponse.get(Constants.PARAMS);
        if (MapUtils.isNotEmpty(params) && StringUtils.isNotBlank((String) params.get(Constants.ERROR_MESSAGE))) {
            return (String) params.get(Constants.ERROR_MESSAGE);
        }
        return Constants.NON_GOVT_USER_CREATE_ERROR;
    }
}
