package org.sunbird.programcoordinator.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
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
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.storage.service.StorageService;

import static org.sunbird.common.util.Constants.ERROR_REQUIRED_ROLE_PREFIX;

/**
 * Sync phase of the Program Coordinator bulk-upload flow. Mirrors
 * org.sunbird.nongovtuser.service.NonGovtUserBulkUploadServiceImpl's shape (access check,
 * in-progress check, structural validation, raw-file upload, tracking-record insert, Kafka
 * trigger), scoped to the 3-column (Registered Name, Registered Email ID, Trainer Type)
 * coordinator schema. Per-row processing (the consumer side) is a separate layer,
 * ProgramCoordinatorBulkUploadProcessingServiceImpl.
 */
@RequiredArgsConstructor
@Service
public class ProgramCoordinatorBulkUploadServiceImpl implements ProgramCoordinatorBulkUploadService {

    private static final Logger logger = LoggerFactory.getLogger(ProgramCoordinatorBulkUploadServiceImpl.class);

    @Value("#{'${program.coordinator.allowed.roles}'.split(',')}")
    private List<String> allowedRoles;

    @Value("${program.coordinator.bulk.upload.max.rows:500}")
    private int maxRows;

    @Value("${kafka.topics.program.coordinator.bulk.upload}")
    private String bulkUploadTopic;

    private final CassandraOperation cassandraOperation;
    private final StorageService storageService;
    private final Producer kafkaProducer;
    private final CbExtServerProperties serverProperties;
    private final AccessTokenValidator accessTokenValidator;

    @Override
    public SBApiResponse bulkUpload(String programId, MultipartFile file, String userAuthToken) {
        logger.info("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: Started for programId: {}", programId);
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_PROGRAM_COORDINATOR_BULK_UPLOAD);
        try {
            List<String> userRoles = accessTokenValidator.fetchUserRolesFromToken(userAuthToken);
            boolean hasAccess = userRoles.stream().anyMatch(allowedRoles::contains);
            if (!hasAccess) {
                markResponseFailed(response, ERROR_REQUIRED_ROLE_PREFIX + String.join(", ", allowedRoles),
                        HttpStatus.FORBIDDEN);
                return response;
            }

            String userId = accessTokenValidator.fetchUserIdFromAccessToken(userAuthToken);

            if (isPreviousUploadInProgress(programId)) {
                logger.warn("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: Rejected for programId: {}, "
                        + "previous upload still in progress", programId);
                markResponseFailed(response, Constants.PC_BULK_UPLOAD_IN_PROGRESS_ERROR, HttpStatus.TOO_MANY_REQUESTS);
                return response;
            }

            String fileValidationError = validateFileStructure(file);
            if (StringUtils.isNotBlank(fileValidationError)) {
                logger.error("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: File validation failed for "
                        + "programId: {}, reason: {}", programId, fileValidationError);
                markResponseFailed(response, fileValidationError, HttpStatus.BAD_REQUEST);
                return response;
            }

            SBApiResponse uploadResponse = uploadRawFileToStorage(file);
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                String uploadFailureMessage = Constants.PC_BULK_UPLOAD_FILE_UPLOAD_ERROR + " "
                        + uploadResponse.getParams().getErrmsg();
                logger.error("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: Raw file upload failed for "
                        + "programId: {}, reason: {}", programId, uploadFailureMessage);
                markResponseFailed(response, uploadFailureMessage, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            Map<String, Object> trackingRecord = buildTrackingRecord(programId, userId, uploadResponse);
            if (!persistTrackingRecord(trackingRecord)) {
                logger.error("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: Failed to persist tracking "
                        + "record for programId: {}, identifier: {}", programId, trackingRecord.get(Constants.IDENTIFIER));
                markResponseFailed(response, Constants.PC_BULK_UPLOAD_DB_INSERT_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(trackingRecord);

            triggerBulkUploadKafkaEvent(trackingRecord, programId, userAuthToken);
            logger.info("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: Successfully queued upload for "
                    + "programId: {}, identifier: {}", programId, trackingRecord.get(Constants.IDENTIFIER));
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadServiceImpl:: bulkUpload: Failed for programId: {}, Error: ", programId, e);
            markResponseFailed(response, Constants.PC_BULK_UPLOAD_PROCESSING_ERROR + " " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse getStatus(String programId, String identifier, String userAuthToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_PROGRAM_COORDINATOR_BULK_UPLOAD_STATUS);
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.PROGRAM_ID, programId);
            compositeKeys.put(Constants.IDENTIFIER, identifier);
            List<Map<String, Object>> records = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PROGRAM_COORDINATOR_BULK_UPLOAD, compositeKeys, null);
            if (CollectionUtils.isEmpty(records)) {
                markResponseFailed(response, Constants.PC_BULK_UPLOAD_NOT_FOUND_ERROR, HttpStatus.NOT_FOUND);
                return response;
            }
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(records.get(0));
        } catch (Exception e) {
            logger.error("ProgramCoordinatorBulkUploadServiceImpl:: getStatus: Failed for programId: {}, identifier: {}",
                    programId, identifier, e);
            markResponseFailed(response, Constants.PC_BULK_UPLOAD_PROCESSING_ERROR + " " + e.getMessage(),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * True if a program coordinator bulk upload for this programme is already IN-PROGRESS
     * (or freshly INITIATED and not yet picked up).
     */
    private boolean isPreviousUploadInProgress(String programId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.PROGRAM_ID, programId);
        List<String> fields = Arrays.asList(Constants.PROGRAM_ID, Constants.IDENTIFIER, Constants.STATUS);

        List<Map<String, Object>> existingUploads = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PROGRAM_COORDINATOR_BULK_UPLOAD, propertyMap, fields);
        if (CollectionUtils.isEmpty(existingUploads)) {
            return false;
        }
        return existingUploads.stream().anyMatch(entry ->
                Constants.INITIATED_CAPITAL.equalsIgnoreCase((String) entry.get(Constants.STATUS))
                        || Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase((String) entry.get(Constants.STATUS)));
    }

    /**
     * Dispatches structural validation by file extension - CSV or XLSX, matching what the
     * async processing side (ProgramCoordinatorBulkUploadProcessingServiceImpl) supports.
     * Returns null when valid, or a caller-facing error message otherwise. Individual rows
     * are NOT rejected here - that's the async layer's job, so valid rows can still succeed
     * even when some rows have errors.
     */
    private String validateFileStructure(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            return Constants.PC_BULK_UPLOAD_FILE_NOT_FOUND_ERROR;
        }
        String extension = getFileExtension(file.getOriginalFilename());
        if (Constants.CSV_FILE.equalsIgnoreCase(extension)) {
            return validateCsvStructure(file);
        }
        if (Constants.XLSX_FILE.equalsIgnoreCase(extension)) {
            return validateExcelStructure(file);
        }
        return Constants.PC_BULK_UPLOAD_UNSUPPORTED_FILE_TYPE_ERROR;
    }

    private String getFileExtension(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return StringUtils.EMPTY;
        }
        int lastIndexOfDot = fileName.lastIndexOf('.');
        return lastIndexOfDot == -1 ? StringUtils.EMPTY : fileName.substring(lastIndexOfDot);
    }

    private String validateCsvStructure(MultipartFile file) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(file.getInputStream(), StandardCharsets.UTF_8))) {
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
                List<String> headers = csvParser.getHeaderNames();
                if (!hasMandatoryHeaders(headers)) {
                    return Constants.PC_BULK_UPLOAD_MANDATORY_COLUMNS_MISSING_ERROR;
                }
                if (!csvParser.iterator().hasNext()) {
                    return Constants.PC_BULK_UPLOAD_NO_DATA_ROWS_ERROR;
                }
            }
            return null;
        } catch (IOException e) {
            logger.error("ProgramCoordinatorBulkUploadServiceImpl:: validateCsvStructure: Failed to parse csv file", e);
            return Constants.PC_BULK_UPLOAD_CSV_PARSE_ERROR + " " + e.getMessage();
        }
    }

    private String validateExcelStructure(MultipartFile file) {
        try (InputStream inputStream = file.getInputStream();
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);
            if (ObjectUtils.isEmpty(headerRow)) {
                return Constants.PC_BULK_UPLOAD_MANDATORY_COLUMNS_MISSING_ERROR;
            }
            DataFormatter dataFormatter = new DataFormatter();
            List<String> headers = new ArrayList<>();
            for (Cell cell : headerRow) {
                headers.add(dataFormatter.formatCellValue(cell).trim());
            }
            if (!hasMandatoryHeaders(headers)) {
                return Constants.PC_BULK_UPLOAD_MANDATORY_COLUMNS_MISSING_ERROR;
            }
            if (sheet.getLastRowNum() < 1) {
                return Constants.PC_BULK_UPLOAD_NO_DATA_ROWS_ERROR;
            }
            return null;
        } catch (IOException e) {
            logger.error("ProgramCoordinatorBulkUploadServiceImpl:: validateExcelStructure: Failed to parse excel file", e);
            return Constants.PC_BULK_UPLOAD_XLSX_PARSE_ERROR + " " + e.getMessage();
        }
    }

    private boolean hasMandatoryHeaders(Collection<String> headers) {
        return findMatchingHeader(headers, Constants.PC_BULK_UPLOAD_COLUMN_NAME) != null
                && findMatchingHeader(headers, Constants.PC_BULK_UPLOAD_COLUMN_EMAIL) != null
                && findMatchingHeader(headers, Constants.PC_BULK_UPLOAD_COLUMN_TRAINER_TYPE) != null;
    }

    /**
     * Matches a header case-insensitively, tolerating an optional "(mandatory)" suffix
     * in the template header row.
     */
    static String findMatchingHeader(Collection<String> headers, String columnName) {
        String mandatorySuffixed = columnName + " (mandatory)";
        for (String header : headers) {
            if (header.equalsIgnoreCase(columnName) || header.equalsIgnoreCase(mandatorySuffixed)) {
                return header;
            }
        }
        return null;
    }

    private SBApiResponse uploadRawFileToStorage(MultipartFile file) throws IOException {
        return storageService.uploadFile(file, serverProperties.getBulkUploadContainerName());
    }

    private Map<String, Object> buildTrackingRecord(String programId, String userId, SBApiResponse uploadResponse) {
        Map<String, Object> trackingRecord = new HashMap<>();
        trackingRecord.put(Constants.PROGRAM_ID, programId);
        trackingRecord.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
        trackingRecord.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
        trackingRecord.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
        trackingRecord.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
        trackingRecord.put(Constants.STATUS, Constants.INITIATED_CAPITAL);
        trackingRecord.put(Constants.COMMENT, StringUtils.EMPTY);
        trackingRecord.put(Constants.CREATED_BY, userId);
        return trackingRecord;
    }

    private boolean persistTrackingRecord(Map<String, Object> trackingRecord) {
        SBApiResponse insertResponse = cassandraOperation.insertRecord(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_PROGRAM_COORDINATOR_BULK_UPLOAD, trackingRecord);
        return Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE));
    }

    /**
     * Publishes the Kafka event that triggers async per-row processing. The caller's auth
     * token is carried through since the consumer needs it to look up users, assign roles,
     * update profiles and finally call ProgramCoordinatorService.upsert().
     */
    private void triggerBulkUploadKafkaEvent(Map<String, Object> trackingRecord, String programId, String userAuthToken) {
        Map<String, Object> kafkaPayload = new HashMap<>(trackingRecord);
        kafkaPayload.put(Constants.X_AUTH_TOKEN, userAuthToken);
        kafkaProducer.pushWithKey(bulkUploadTopic, kafkaPayload, programId);
    }

    private void markResponseFailed(SBApiResponse response, String errMsg, HttpStatus status) {
        response.getParams().setStatus(Constants.FAILED_UPPERCASE);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(status);
    }
}
