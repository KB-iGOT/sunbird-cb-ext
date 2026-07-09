package org.sunbird.nongovtuser.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.storage.service.StorageService;

/**
 * Bulk creation API for Non-Govt (Volunteer) users.
 * <p>
 * Mirrors ProfileServiceImpl.bulkUploadV2's shape (in-progress check, structural
 * validation, raw-file upload, tracking-record insert, Kafka trigger), split into
 * single-purpose private methods. Accepts both CSV and XLSX (unlike bulkUploadV2,
 * which is CSV-only at this validation layer) since the async processing side
 * supports both formats. Stops at triggering the Kafka event — per-row processing
 * (the consumer side) is a separate layer.
 */
@RequiredArgsConstructor
@Service
public class NonGovtUserBulkUploadServiceImpl implements NonGovtUserBulkUploadService {

    private static final Logger logger = LoggerFactory.getLogger(NonGovtUserBulkUploadServiceImpl.class);

    private final CassandraOperation cassandraOperation;
    private final StorageService storageService;
    private final Producer kafkaProducer;
    private final CbExtServerProperties serverProperties;

    @Override
    public SBApiResponse bulkUploadNonGovtUsers(MultipartFile mFile, String orgId, String orgName,
                                                 String userId, String userAuthToken) {
        logger.info("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: Started for orgId: {}", orgId);
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_NON_GOVT_USER_BULK_UPLOAD);
        try {
            if (isPreviousUploadInProgress(orgId)) {
                logger.warn("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: Rejected for orgId: {}, "
                        + "previous upload still in progress", orgId);
                markResponseFailed(response, Constants.NON_GOVT_BULK_UPLOAD_IN_PROGRESS_ERROR, HttpStatus.TOO_MANY_REQUESTS);
                return response;
            }
            String fileValidationError = validateFileStructure(mFile);
            if (StringUtils.isNotBlank(fileValidationError)) {
                logger.error("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: File validation failed for "
                        + "orgId: {}, reason: {}", orgId, fileValidationError);
                markResponseFailed(response, fileValidationError, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            SBApiResponse uploadResponse = uploadRawFileToStorage(mFile);
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                String uploadFailureMessage = buildUploadFailureMessage(uploadResponse);
                logger.error("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: Raw file upload failed for "
                        + "orgId: {}, reason: {}", orgId, uploadFailureMessage);
                markResponseFailed(response, uploadFailureMessage, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            Map<String, Object> trackingRecord = buildTrackingRecord(orgId, userId, uploadResponse);
            if (!persistTrackingRecord(trackingRecord)) {
                logger.error("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: Failed to persist tracking "
                        + "record for orgId: {}, identifier: {}", orgId, trackingRecord.get(Constants.IDENTIFIER));
                markResponseFailed(response, Constants.NON_GOVT_BULK_UPLOAD_DB_INSERT_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            populateSuccessResponse(response, trackingRecord);
            triggerBulkUploadKafkaEvent(trackingRecord, orgId, orgName, userAuthToken);
            logger.info("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: Successfully queued upload for "
                    + "orgId: {}, identifier: {}", orgId, trackingRecord.get(Constants.IDENTIFIER));
        } catch (Exception e) {
            logger.error("NonGovtUserBulkUploadServiceImpl:: bulkUploadNonGovtUsers: Failed for orgId: {}, Error: ", orgId, e);
            markResponseFailed(response,
                    Constants.NON_GOVT_BULK_UPLOAD_PROCESSING_ERROR + " " + e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * True if a non-govt bulk upload for this org is already IN-PROGRESS.
     */
    private boolean isPreviousUploadInProgress(String orgId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.ROOT_ORG_ID, orgId);
        List<String> fields = Arrays.asList(Constants.ROOT_ORG_ID, Constants.IDENTIFIER, Constants.STATUS);

        List<Map<String, Object>> existingUploads = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_BULK_UPLOAD, propertyMap, fields);
        if (CollectionUtils.isEmpty(existingUploads)) {
            return false;
        }
        return existingUploads.stream()
                .anyMatch(entry -> Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase((String) entry.get(Constants.STATUS)));
    }

    /**
     * Dispatches structural validation by file extension — CSV or XLSX, matching what
     * the async processing side (NonGovtUserBulkUploadProcessingServiceImpl) supports.
     * Returns null when valid, or a caller-facing error message otherwise.
     */
    private String validateFileStructure(MultipartFile mFile) {
        String extension = getFileExtension(mFile.getOriginalFilename());
        if (Constants.CSV_FILE.equalsIgnoreCase(extension)) {
            return validateCsvStructure(mFile);
        }
        if (Constants.XLSX_FILE.equalsIgnoreCase(extension)) {
            return validateExcelStructure(mFile);
        }
        return Constants.NON_GOVT_BULK_UPLOAD_UNSUPPORTED_FILE_TYPE_ERROR;
    }

    private String getFileExtension(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return StringUtils.EMPTY;
        }
        int lastIndexOfDot = fileName.lastIndexOf('.');
        return lastIndexOfDot == -1 ? StringUtils.EMPTY : fileName.substring(lastIndexOfDot);
    }

    /**
     * Confirms the uploaded file parses as a CSV with a header row containing the
     * mandatory columns.
     */
    private String validateCsvStructure(MultipartFile mFile) {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(mFile.getInputStream(), StandardCharsets.UTF_8))) {
            char csvDelimiter = serverProperties.getCsvDelimiter();
            CSVFormat csvFormat = CSVFormat.newFormat(csvDelimiter).builder()
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build();
            try (CSVParser csvParser = new CSVParser(reader, csvFormat)) {
                List<String> headers = csvParser.getHeaderNames();
                if (!headers.contains(Constants.NON_GOVT_CSV_COLUMN_FULL_NAME)
                        || !headers.contains(Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER)) {
                    return Constants.NON_GOVT_BULK_UPLOAD_MANDATORY_COLUMNS_MISSING_ERROR;
                }
            }
            return null;
        } catch (IOException e) {
            logger.error("NonGovtUserBulkUploadServiceImpl:: validateCsvStructure: Failed to parse csv file", e);
            return Constants.NON_GOVT_BULK_UPLOAD_CSV_PARSE_ERROR + " " + e.getMessage();
        }
    }

    /**
     * Confirms the uploaded file parses as an XLSX workbook whose first sheet's header
     * row contains the mandatory columns.
     */
    private String validateExcelStructure(MultipartFile mFile) {
        try (InputStream inputStream = mFile.getInputStream();
             Workbook workbook = new XSSFWorkbook(inputStream)) {
            Sheet sheet = workbook.getSheetAt(0);
            Row headerRow = sheet.getRow(0);
            if (ObjectUtils.isEmpty(headerRow)) {
                return Constants.NON_GOVT_BULK_UPLOAD_MANDATORY_COLUMNS_MISSING_ERROR;
            }
            Set<String> headers = new HashSet<>();
            DataFormatter dataFormatter = new DataFormatter();
            for (Cell cell : headerRow) {
                headers.add(dataFormatter.formatCellValue(cell).trim());
            }
            if (!headers.contains(Constants.NON_GOVT_CSV_COLUMN_FULL_NAME)
                    || !headers.contains(Constants.NON_GOVT_CSV_COLUMN_MOBILE_NUMBER)) {
                return Constants.NON_GOVT_BULK_UPLOAD_MANDATORY_COLUMNS_MISSING_ERROR;
            }
            return null;
        } catch (IOException e) {
            logger.error("NonGovtUserBulkUploadServiceImpl:: validateExcelStructure: Failed to parse excel file", e);
            return Constants.NON_GOVT_BULK_UPLOAD_XLSX_PARSE_ERROR + " " + e.getMessage();
        }
    }

    /**
     * Uploads the raw file as-is to cloud storage; the caller checks the response code.
     */
    private SBApiResponse uploadRawFileToStorage(MultipartFile mFile) throws IOException {
        return storageService.uploadFile(mFile, serverProperties.getBulkUploadContainerName());
    }

    private String buildUploadFailureMessage(SBApiResponse uploadResponse) {
        return Constants.NON_GOVT_BULK_UPLOAD_FILE_UPLOAD_ERROR + " " + uploadResponse.getParams().getErrmsg();
    }

    /**
     * Builds the tracking-record fields written to TABLE_USER_BULK_UPLOAD (shared with
     * the govt bulk-upload flow for now).
     */
    private Map<String, Object> buildTrackingRecord(String orgId, String userId, SBApiResponse uploadResponse) {
        Map<String, Object> trackingRecord = new HashMap<>();
        trackingRecord.put(Constants.ROOT_ORG_ID, orgId);
        trackingRecord.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
        trackingRecord.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
        trackingRecord.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
        trackingRecord.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
        trackingRecord.put(Constants.STATUS, Constants.INITIATED_CAPITAL);
        trackingRecord.put(Constants.COMMENT, StringUtils.EMPTY);
        trackingRecord.put(Constants.CREATED_BY, userId);
        return trackingRecord;
    }

    /**
     * Persists the tracking record; returns whether the insert succeeded.
     */
    private boolean persistTrackingRecord(Map<String, Object> trackingRecord) {
        SBApiResponse insertResponse = cassandraOperation.insertRecord(
                Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_BULK_UPLOAD, trackingRecord);
        return Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE));
    }

    private void populateSuccessResponse(SBApiResponse response, Map<String, Object> trackingRecord) {
        response.getParams().setStatus(Constants.SUCCESSFUL);
        response.setResponseCode(HttpStatus.OK);
        response.getResult().putAll(trackingRecord);
    }

    /**
     * Publishes the Kafka event that will trigger async per-row processing (consumer
     * side not yet built). Builds a dedicated payload map rather than mutating
     * trackingRecord, since that map is also used to populate the HTTP response.
     */
    private void triggerBulkUploadKafkaEvent(Map<String, Object> trackingRecord, String orgId,
                                              String orgName, String userAuthToken) {
        Map<String, Object> kafkaPayload = new HashMap<>(trackingRecord);
        kafkaPayload.put(Constants.ORG_NAME, orgName);
        kafkaPayload.put(Constants.X_AUTH_TOKEN, userAuthToken);
        kafkaProducer.pushWithKey(serverProperties.getNonGovtUserBulkUploadTopic(), kafkaPayload, orgId);
    }

    private void markResponseFailed(SBApiResponse response, String errMsg, HttpStatus status) {
        response.getParams().setStatus(Constants.FAILED_UPPERCASE);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(status);
    }
}
