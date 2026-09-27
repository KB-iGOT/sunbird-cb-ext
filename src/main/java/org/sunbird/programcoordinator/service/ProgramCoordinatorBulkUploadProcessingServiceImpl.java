package org.sunbird.programcoordinator.service;

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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorRoleEntity;
import org.sunbird.programcoordinator.model.ProgramCoordinatorBulkUploadRowSummary;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRoleRepository;
import org.sunbird.storage.service.StorageService;
import org.sunbird.user.service.UserUtilityService;

/**
 * Async processing side of the Program Coordinator bulk upload. Mirrors
 * org.sunbird.nongovtuser.service.NonGovtUserBulkUploadProcessingServiceImpl's shape (download,
 * parse CSV/XLSX, validate + process every row without aborting the file on one bad row, write
 * an annotated results file, persist a terminal status), but per valid row it additionally:
 * assigns the BP_PROGRAM_TRAINER role (same fetch-roles/append/re-POST pattern as
 * OperationalReportServiceImpl.grantReportAccessToMDOAdmin), merges profileDetails.bpCoTrainer
 * (same fetch-existing/mutate-one-key/PATCH pattern as ProfileServiceImpl's profile update), and
 * batches a ProgramCoordinatorUpsertRequest for a single ProgramCoordinatorService.upsert() call
 * covering every valid row in the file.
 */
@RequiredArgsConstructor
@Service
public class ProgramCoordinatorBulkUploadProcessingServiceImpl implements ProgramCoordinatorBulkUploadProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(ProgramCoordinatorBulkUploadProcessingServiceImpl.class);
    private static final DataFormatter EXCEL_CELL_FORMATTER = new DataFormatter();

    @Value("${program.coordinator.bulk.upload.max.rows:500}")
    private int maxRows;

    @Value("${program.coordinator.bulk.upload.result.headers}")
    private String resultHeadersConfig;

    private final ObjectMapper objectMapper;
    private final CassandraOperation cassandraOperation;
    private final StorageService storageService;
    private final CbExtServerProperties serverProperties;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final UserUtilityService userUtilityService;
    private final ProgramCoordinatorRoleRepository programCoordinatorRoleRepository;
    private final ProgramCoordinatorService programCoordinatorService;

    private List<String> getResultHeaders() {
        return Arrays.stream(resultHeadersConfig.split(",")).map(String::trim).collect(Collectors.toList());
    }

    @Override
    public void initiateProgramCoordinatorBulkUploadProcess(String inputData) {
        logger.info("ProgramCoordinatorBulkUploadProcessingServiceImpl:: initiateProgramCoordinatorBulkUploadProcess: Started");
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
                logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: initiateProgramCoordinatorBulkUploadProcess: "
                        + "Invalid Kafka message for identifier: {}, errors: {}", identifier, validationErrors);
                return;
            }
            updateStatus(inputDataMap.get(Constants.PROGRAM_ID), identifier, Constants.STATUS_IN_PROGRESS_UPPERCASE, -1, -1, -1);
            processBulkUpload(inputDataMap);
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: initiateProgramCoordinatorBulkUploadProcess: Failed", e);
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            logger.info("ProgramCoordinatorBulkUploadProcessingServiceImpl:: initiateProgramCoordinatorBulkUploadProcess: "
                    + "Completed for identifier: {}. Time taken: {} ms", identifier, duration);
        }
    }

    private Map<String, String> deserializeKafkaMessage(String inputData) {
        try {
            return objectMapper.readValue(inputData, new TypeReference<Map<String, String>>() {
            });
        } catch (IOException e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: deserializeKafkaMessage: Failed to parse Kafka message", e);
            return Collections.emptyMap();
        }
    }

    private List<String> validateReceivedMessage(Map<String, String> inputDataMap) {
        List<String> errors = new ArrayList<>();
        if (StringUtils.isBlank(inputDataMap.get(Constants.PROGRAM_ID))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.PROGRAM_ID));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.IDENTIFIER))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.IDENTIFIER));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.FILE_NAME))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.FILE_NAME));
        }
        if (StringUtils.isBlank(inputDataMap.get(Constants.X_AUTH_TOKEN))) {
            errors.add(String.format(Constants.FIELD_NOT_PRESENT_ERROR, Constants.X_AUTH_TOKEN));
        }
        return errors;
    }

    private void processBulkUpload(Map<String, String> inputDataMap) {
        String programId = inputDataMap.get(Constants.PROGRAM_ID);
        String identifier = inputDataMap.get(Constants.IDENTIFIER);
        logger.info("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processBulkUpload: Started for identifier: {}, programId: {}",
                identifier, programId);
        File file = downloadUploadedFile(inputDataMap.get(Constants.FILE_NAME));
        if (ObjectUtils.isEmpty(file)) {
            updateStatus(programId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
            return;
        }
        try {
            processDownloadedFile(file, programId, identifier, inputDataMap);
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processBulkUpload: Failed for identifier: {}", identifier, e);
            updateStatus(programId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
        } finally {
            deleteLocalFile(file);
        }
    }

    private void deleteLocalFile(File file) {
        try {
            if (file != null && file.exists()) {
                Files.delete(file.toPath());
            }
        } catch (IOException e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: deleteLocalFile: Failed to delete file: {}", file.getPath(), e);
        }
    }

    private File downloadUploadedFile(String fileName) {
        storageService.downloadFile(fileName);
        File file = new File(Constants.LOCAL_BASE_PATH + fileName);
        if (!file.exists() || file.length() == 0) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: downloadUploadedFile: {}",
                    Constants.PC_BULK_UPLOAD_FILE_NOT_FOUND_ERROR);
            return null;
        }
        return file;
    }

    private void processDownloadedFile(File file, String programId, String identifier,
                                        Map<String, String> inputDataMap) throws IOException {
        String fileName = inputDataMap.get(Constants.FILE_NAME);
        String extension = getFileExtension(fileName);
        List<Map<String, String>> rawRows;
        if (Constants.CSV_FILE.equalsIgnoreCase(extension)) {
            rawRows = extractCsvRows(file);
        } else if (Constants.XLSX_FILE.equalsIgnoreCase(extension)) {
            rawRows = extractExcelRows(file);
        } else {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                    + "unsupported file type: {}", identifier, fileName);
            updateStatus(programId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
            return;
        }
        if (CollectionUtils.isEmpty(rawRows)) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                    + "no rows found in uploaded file", identifier);
            updateStatus(programId, identifier, Constants.FAILED_UPPERCASE, 0, 0, 0);
            return;
        }
        if (rawRows.size() > maxRows) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                    + "row count {} exceeds the maximum allowed rows of {}", identifier, rawRows.size(), maxRows);
            updateStatus(programId, identifier, Constants.FAILED_UPPERCASE, rawRows.size(), 0, rawRows.size());
            return;
        }
        String token = inputDataMap.get(Constants.X_AUTH_TOKEN);
        Map<String, Short> roleCodeToId = loadActiveTrainerRoles();
        ProgramCoordinatorBulkUploadRowSummary summary = processRows(rawRows, programId, token, roleCodeToId);
        logger.info("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processDownloadedFile: identifier: {}, "
                        + "total: {}, successful: {}, failed: {}",
                identifier, summary.getTotalRecords(), summary.getSuccessfulRecords(), summary.getFailedRecords());
        String resultFileUrl = writeAndUploadResults(file, extension, summary.getUpdatedRecords());
        persistFinalOutcome(programId, identifier, summary, resultFileUrl);
    }

    private String getFileExtension(String fileName) {
        int lastIndexOfDot = fileName.lastIndexOf('.');
        return lastIndexOfDot == -1 ? StringUtils.EMPTY : fileName.substring(lastIndexOfDot);
    }

    /**
     * roleCode (upper-cased) -> roleId, sourced from program_coordinator_role, excluding the
     * base "Program Coordinator" role - same filter ProgramCoordinatorServiceImpl.getCoordinatorRoles
     * already applies. These are exactly the values a "Trainer Type" cell may contain.
     */
    private Map<String, Short> loadActiveTrainerRoles() {
        return programCoordinatorRoleRepository.findAll().stream()
                .filter(role -> Boolean.TRUE.equals(role.getIsActive()))
                .filter(role -> !Constants.PROGRAM_COORDINATOR_KEY.equalsIgnoreCase(role.getRoleName()))
                .filter(role -> StringUtils.isNotBlank(role.getRoleCode()))
                .collect(Collectors.toMap(role -> role.getRoleCode().toUpperCase(), ProgramCoordinatorRoleEntity::getId));
    }

    // ---------------------------------------------------------------- row extraction (CSV/XLSX)

    private List<Map<String, String>> extractCsvRows(File file) {
        List<CSVRecord> records = parseCsvRecords(file);
        List<Map<String, String>> rows = new ArrayList<>();
        if (records.isEmpty()) {
            return rows;
        }
        Set<String> headers = records.get(0).toMap().keySet();
        String nameHeader = ProgramCoordinatorBulkUploadServiceImpl.findMatchingHeader(headers, Constants.PC_BULK_UPLOAD_COLUMN_NAME);
        String emailHeader = ProgramCoordinatorBulkUploadServiceImpl.findMatchingHeader(headers, Constants.PC_BULK_UPLOAD_COLUMN_EMAIL);
        String trainerTypeHeader = ProgramCoordinatorBulkUploadServiceImpl.findMatchingHeader(headers, Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE);
        for (CSVRecord csvRecord : records) {
            Map<String, String> row = new HashMap<>();
            row.put(Constants.PC_BULK_UPLOAD_COLUMN_NAME, getCsvFieldValue(csvRecord, nameHeader));
            row.put(Constants.PC_BULK_UPLOAD_COLUMN_EMAIL, getCsvFieldValue(csvRecord, emailHeader));
            row.put(Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE, getCsvFieldValue(csvRecord, trainerTypeHeader));
            rows.add(row);
        }
        return rows;
    }

    private List<CSVRecord> parseCsvRecords(File file) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8))) {
            char csvDelimiter = serverProperties.getCsvDelimiter();
            CSVFormat csvFormat = CSVFormat.RFC4180.builder()
                    .setDelimiter(csvDelimiter)
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .setQuote('"')
                    .setIgnoreSurroundingSpaces(true)
                    .setTrim(true)
                    .build();
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                return csvParser.getRecords();
            }
        } catch (IOException e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: parseCsvRecords: Failed to parse csv file", e);
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
            String nameHeader = ProgramCoordinatorBulkUploadServiceImpl.findMatchingHeader(columnIndexByHeader.keySet(), Constants.PC_BULK_UPLOAD_COLUMN_NAME);
            String emailHeader = ProgramCoordinatorBulkUploadServiceImpl.findMatchingHeader(columnIndexByHeader.keySet(), Constants.PC_BULK_UPLOAD_COLUMN_EMAIL);
            String trainerTypeHeader = ProgramCoordinatorBulkUploadServiceImpl.findMatchingHeader(columnIndexByHeader.keySet(), Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE);
            List<Map<String, String>> rows = new ArrayList<>();
            for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
                Row row = sheet.getRow(rowIndex);
                if (ObjectUtils.isEmpty(row)) {
                    continue;
                }
                Map<String, String> rowData = new HashMap<>();
                rowData.put(Constants.PC_BULK_UPLOAD_COLUMN_NAME, getExcelCellValue(row, columnIndexByHeader, nameHeader));
                rowData.put(Constants.PC_BULK_UPLOAD_COLUMN_EMAIL, getExcelCellValue(row, columnIndexByHeader, emailHeader));
                rowData.put(Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE, getExcelCellValue(row, columnIndexByHeader, trainerTypeHeader));
                rows.add(rowData);
            }
            return rows;
        } catch (IOException e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: extractExcelRows: Failed to parse excel file", e);
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

    private String getExcelCellValue(Row row, Map<String, Integer> columnIndexByHeader, String columnName) {
        Integer columnIndex = columnName == null ? null : columnIndexByHeader.get(columnName);
        if (ObjectUtils.isEmpty(columnIndex)) {
            return StringUtils.EMPTY;
        }
        Cell cell = row.getCell(columnIndex);
        return ObjectUtils.isEmpty(cell) ? StringUtils.EMPTY : EXCEL_CELL_FORMATTER.formatCellValue(cell).trim();
    }

    // ---------------------------------------------------------------------------- row processing

    private ProgramCoordinatorBulkUploadRowSummary processRows(List<Map<String, String>> rawRows, String programId,
                                                                String token, Map<String, Short> roleCodeToId) {
        ProgramCoordinatorBulkUploadRowSummary summary = new ProgramCoordinatorBulkUploadRowSummary();
        Set<String> seenEmails = new HashSet<>();
        List<ProgramCoordinatorUpsertRequest> batch = new ArrayList<>();
        Map<String, Map<String, String>> rowByUserId = new HashMap<>();
        List<Map<String, String>> rowResults = new ArrayList<>();

        for (Map<String, String> rawRow : rawRows) {
            Map<String, String> updatedRecord = new HashMap<>(rawRow);
            rowResults.add(updatedRecord);
            if (isRowCompletelyEmpty(rawRow)) {
                rowResults.remove(updatedRecord);
                continue;
            }
            try {
                String userId = validateAndPrepareRow(rawRow, updatedRecord, seenEmails, programId, token, roleCodeToId, batch);
                if (userId != null) {
                    rowByUserId.put(userId, updatedRecord);
                }
            } catch (Exception e) {
                logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: processRows: Unexpected error for a row", e);
                markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_ROW_PROCESSING_ERROR);
            }
        }

        if (!batch.isEmpty()) {
            reconcileUpsert(programId, token, batch, rowByUserId);
        }
        rowResults.forEach(summary::recordRow);
        return summary;
    }

    private boolean isRowCompletelyEmpty(Map<String, String> rawRow) {
        return StringUtils.isBlank(rawRow.get(Constants.PC_BULK_UPLOAD_COLUMN_NAME))
                && StringUtils.isBlank(rawRow.get(Constants.PC_BULK_UPLOAD_COLUMN_EMAIL))
                && StringUtils.isBlank(rawRow.get(Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE));
    }

    /**
     * Validates one row, and if it passes, assigns the BP_PROGRAM_TRAINER role, merges
     * profileDetails.bpCoTrainer, and adds a batch entry for the final single upsert() call.
     * Never throws for expected validation failures - those are recorded on updatedRecord and
     * this method returns null so the row is excluded from the batch. Returns the resolved
     * userId (uppercased trimmed) when the row was added to the batch, so the caller can
     * reconcile upsert()'s result back onto this row.
     */
    private String validateAndPrepareRow(Map<String, String> rawRow, Map<String, String> updatedRecord,
                                          Set<String> seenEmails, String programId, String token,
                                          Map<String, Short> roleCodeToId, List<ProgramCoordinatorUpsertRequest> batch) {
        String name = rawRow.get(Constants.PC_BULK_UPLOAD_COLUMN_NAME);
        String email = rawRow.get(Constants.PC_BULK_UPLOAD_COLUMN_EMAIL);
        String trainerType = rawRow.get(Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE);

        if (StringUtils.isBlank(name) || StringUtils.isBlank(email) || StringUtils.isBlank(trainerType)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_MANDATORY_VALUE_MISSING_ERROR);
            return null;
        }
        if (StringUtils.isNotBlank(userUtilityService.emailValidation(email, false))) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_INVALID_EMAIL_ERROR);
            return null;
        }
        String normalizedEmail = email.toLowerCase();
        if (!seenEmails.add(normalizedEmail)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_DUPLICATE_EMAIL_ERROR);
            return null;
        }
        Short roleId = roleCodeToId.get(trainerType.trim().toUpperCase());
        if (roleId == null) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_INVALID_TRAINER_TYPE_ERROR);
            return null;
        }

        Map<String, Object> lookedUpUser = userUtilityService.getUsersDataFromLookup(email, token);
        if (MapUtils.isEmpty(lookedUpUser)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_USER_NOT_REGISTERED_ERROR);
            return null;
        }
        String userId = (String) lookedUpUser.get(Constants.ID);
        if (StringUtils.isBlank(userId)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_USER_NOT_REGISTERED_ERROR);
            return null;
        }

        Map<String, Object> userData = userUtilityService.getUsersReadData(userId, token, token);
        if (MapUtils.isEmpty(userData)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_USER_NOT_REGISTERED_ERROR);
            return null;
        }
        if (!registeredNameMatches(name, userData)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_NAME_MISMATCH_ERROR);
            return null;
        }

        String trainerTypeCode = trainerType.trim().toUpperCase();
        if (!assignTrainerRole(userId, token)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_ROLE_ASSIGN_FAILED_ERROR);
            return null;
        }
        if (!updateBpCoTrainerProfile(userId, trainerTypeCode, token)) {
            markRowFailed(updatedRecord, Constants.PC_BULK_UPLOAD_PROFILE_UPDATE_FAILED_ERROR);
            return null;
        }

        ProgramCoordinatorUpsertRequest upsertRequest = new ProgramCoordinatorUpsertRequest();
        upsertRequest.setUserId(UUID.fromString(userId));
        upsertRequest.setRoleId(roleId);
        upsertRequest.setStatus(Constants.ACTIVE_STATUS_PC);
        batch.add(upsertRequest);
        return userId;
    }

    /**
     * Compares the file's "Registered Name" against profileDetails.personalDetails.firstname on
     * the registered user's record.
     */
    private boolean registeredNameMatches(String fileName, Map<String, Object> userData) {
        String registeredName = null;
        Object profileDetailsObj = userData.get(Constants.PROFILE_DETAILS);
        if (profileDetailsObj instanceof Map) {
            Object personalDetailsObj = ((Map<String, Object>) profileDetailsObj).get(Constants.PERSONAL_DETAILS);
            if (personalDetailsObj instanceof Map) {
                Object firstName = ((Map<String, Object>) personalDetailsObj).get("firstname");
                registeredName = firstName == null ? null : firstName.toString();
            }
        }
        return StringUtils.isNotBlank(registeredName) && registeredName.trim().equalsIgnoreCase(fileName.trim());
    }

    /**
     * Fetch-existing-roles/append-one/re-POST-the-whole-list pattern, same as
     * OperationalReportServiceImpl.grantReportAccessToMDOAdmin, generalized to
     * BP_PROGRAM_TRAINER. No-op (returns true) if the user already holds the role.
     * <p>
     * Re-fetches the user's roles immediately before acting rather than reusing a snapshot taken
     * earlier in row processing, so this always appends onto the truly-current server state
     * instead of a possibly-stale copy.
     */
    private boolean assignTrainerRole(String userId, String token) {
        try {
            Map<String, Object> userData = userUtilityService.getUsersReadData(userId, token, token);
            if (MapUtils.isEmpty(userData)) {
                return false;
            }
            List<String> roles = (List<String>) userData.get(Constants.ROLES);
            List<String> currentRoles = roles == null ? new ArrayList<>() : new ArrayList<>(roles);
            if (currentRoles.contains(Constants.BP_PROGRAM_TRAINER)) {
                return true;
            }
            currentRoles.add(Constants.BP_PROGRAM_TRAINER);
            String orgId = (String) userData.get(Constants.ROOT_ORG_ID);

            Map<String, Object> roleRequestBody = new HashMap<>();
            roleRequestBody.put(Constants.ORGANIZATION_ID, orgId);
            roleRequestBody.put(Constants.USER_ID, userId);
            roleRequestBody.put(Constants.ROLES, currentRoles);
            Map<String, Object> assignRoleReq = new HashMap<>();
            assignRoleReq.put(Constants.REQUEST, roleRequestBody);

            // No x-authenticated-user-token here - /v1/user/assign/role is a system/private LMS
            // endpoint and doesn't require the caller's token.
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);

            Map<String, Object> assignRoleResp = outboundRequestHandlerService.fetchResultUsingPost(
                    serverProperties.getSbUrl() + serverProperties.getSbAssignRolePathV2(), assignRoleReq, headers);
            return Constants.OK.equalsIgnoreCase((String) assignRoleResp.get(Constants.RESPONSE_CODE));
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: assignTrainerRole: Failed for userId: {}", userId, e);
            return false;
        }
    }

    /**
     * Fetch-existing-profileDetails/mutate-one-key/PATCH-the-whole-thing pattern, same as
     * ProfileServiceImpl's profile update - this is what preserves every other profileDetails
     * field untouched.
     * <p>
     * Re-fetches profileDetails immediately before acting (rather than reusing the snapshot from
     * before assignTrainerRole ran), so this always merges onto the truly-current server state.
     */
    private boolean updateBpCoTrainerProfile(String userId, String trainerTypeCode, String token) {
        try {
            Map<String, Object> userData = userUtilityService.getUsersReadData(userId, token, token);
            if (MapUtils.isEmpty(userData)) {
                return false;
            }
            Object profileDetailsObj = userData.get(Constants.PROFILE_DETAILS);
            Map<String, Object> profileDetails = profileDetailsObj instanceof Map
                    ? new HashMap<>((Map<String, Object>) profileDetailsObj) : new HashMap<>();
            profileDetails.put(Constants.BP_CO_TRAINER, trainerTypeCode);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put(Constants.USER_ID, userId);
            requestBody.put(Constants.PROFILE_DETAILS, profileDetails);
            Map<String, Object> updateRequest = new HashMap<>();
            updateRequest.put(Constants.REQUEST, requestBody);

            // No x-authenticated-user-token here - /private/user/v1/update is a system/private
            // LMS endpoint and doesn't require the caller's token.
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);

            Map<String, Object> updateResponse = outboundRequestHandlerService.fetchResultUsingPatch(
                    serverProperties.getSbUrl() + serverProperties.getLmsUserUpdatePrivatePath(), updateRequest, headers);
            return Constants.OK.equalsIgnoreCase((String) updateResponse.get(Constants.RESPONSE_CODE));
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: updateBpCoTrainerProfile: Failed for userId: {}", userId, e);
            return false;
        }
    }

    /**
     * Runs the existing single-user upsert() once for every row that passed validation - the
     * "single action" add. Since upsert() doesn't report per-row failure (only an overall
     * response plus which userIds actually changed a DB row), every batched row is marked
     * Successful when the call as a whole succeeds - including rows where the user was already
     * an active coordinator on this programme, since that end-state is exactly what the row
     * asked for - and marked Failed with the response's error when the call as a whole fails.
     */
    private void reconcileUpsert(String programId, String token, List<ProgramCoordinatorUpsertRequest> batch,
                                  Map<String, Map<String, String>> rowByUserId) {
        try {
            SBApiResponse upsertResponse = programCoordinatorService.upsert(programId, batch, token);
            boolean succeeded = HttpStatus.OK.equals(upsertResponse.getResponseCode());
            for (ProgramCoordinatorUpsertRequest request : batch) {
                Map<String, String> row = rowByUserId.get(request.getUserId().toString());
                if (row == null) {
                    continue;
                }
                if (succeeded) {
                    markRowSuccessful(row);
                } else {
                    String errMsg = upsertResponse.getParams() != null ? upsertResponse.getParams().getErrmsg() : null;
                    markRowFailed(row, Constants.PC_BULK_UPLOAD_ROW_PROCESSING_ERROR + " " + StringUtils.defaultString(errMsg));
                }
            }
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: reconcileUpsert: Failed for programId: {}", programId, e);
            batch.forEach(request -> {
                Map<String, String> row = rowByUserId.get(request.getUserId().toString());
                if (row != null) {
                    markRowFailed(row, Constants.PC_BULK_UPLOAD_ROW_PROCESSING_ERROR + " " + e.getMessage());
                }
            });
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

    // ------------------------------------------------------------------------------ result file

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
        List<String> resultHeaders = getResultHeaders();
        CSVFormat outputFormat = CSVFormat.RFC4180.builder()
                .setDelimiter(csvDelimiter)
                .setHeader(resultHeaders.toArray(new String[0]))
                .setRecordSeparator(System.lineSeparator())
                .setQuote('"')
                .setQuoteMode(org.apache.commons.csv.QuoteMode.MINIMAL)
                .build();
        try (FileWriter fileWriter = new FileWriter(file);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
             CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, outputFormat)) {
            for (Map<String, String> rowData : updatedRecords) {
                List<String> row = new ArrayList<>();
                for (String header : resultHeaders) {
                    row.add(rowData.getOrDefault(header, StringUtils.EMPTY));
                }
                csvPrinter.printRecord(row);
            }
        }
    }

    private void writeResultExcel(File file, List<Map<String, String>> updatedRecords) throws IOException {
        try (Workbook workbook = new XSSFWorkbook()) {
            Sheet sheet = workbook.createSheet();
            List<String> resultHeaders = getResultHeaders();
            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < resultHeaders.size(); i++) {
                headerRow.createCell(i).setCellValue(resultHeaders.get(i));
            }
            int rowIndex = 1;
            for (Map<String, String> rowData : updatedRecords) {
                Row row = sheet.createRow(rowIndex++);
                for (int i = 0; i < resultHeaders.size(); i++) {
                    row.createCell(i).setCellValue(rowData.getOrDefault(resultHeaders.get(i), StringUtils.EMPTY));
                }
            }
            try (FileOutputStream fos = new FileOutputStream(file)) {
                workbook.write(fos);
            }
        }
    }

    private String uploadResultFile(File file) {
        SBApiResponse uploadResponse = storageService.uploadFile(
                file, serverProperties.getBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: uploadResultFile: {}",
                    Constants.PC_BULK_UPLOAD_FILE_UPLOAD_ERROR);
            return null;
        }
        return (String) uploadResponse.getResult().get(Constants.URL);
    }

    // ----------------------------------------------------------------------------- final outcome

    private void persistFinalOutcome(String programId, String identifier, ProgramCoordinatorBulkUploadRowSummary summary,
                                      String resultFileUrl) {
        String finalStatus = determineFinalStatus(summary);
        updateStatus(programId, identifier, finalStatus,
                summary.getTotalRecords(), summary.getSuccessfulRecords(), summary.getFailedRecords());
        if (StringUtils.isNotBlank(resultFileUrl)) {
            updateResultFilePath(programId, identifier, resultFileUrl);
        }
    }

    private String determineFinalStatus(ProgramCoordinatorBulkUploadRowSummary summary) {
        if (summary.getTotalRecords() == 0 || summary.getSuccessfulRecords() == 0) {
            return Constants.FAILED_UPPERCASE;
        }
        if (summary.getFailedRecords() == 0) {
            return Constants.SUCCESSFUL_UPPERCASE;
        }
        return Constants.STATUS_PARTIALLY_COMPLETED_UPPERCASE;
    }

    private void updateResultFilePath(String programId, String identifier, String resultFileUrl) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.PROGRAM_ID, programId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);
            Map<String, Object> fieldsToBeUpdated = new HashMap<>();
            fieldsToBeUpdated.put(Constants.RESULT_FILE_PATH, resultFileUrl);
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PROGRAM_COORDINATOR_BULK_UPLOAD,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: updateResultFilePath: Failed for identifier: {}", identifier, e);
        }
    }

    private void updateStatus(String programId, String identifier, String status,
                               int totalRecordsCount, int successfulRecordsCount, int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.PROGRAM_ID, programId);
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
            fieldsToBeUpdated.put(Constants.DATE_UPDATE_ON, Timestamp.from(Instant.now()));

            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PROGRAM_COORDINATOR_BULK_UPLOAD,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadProcessingServiceImpl:: updateStatus: Failed for identifier: {}", identifier, e);
        }
    }
}
