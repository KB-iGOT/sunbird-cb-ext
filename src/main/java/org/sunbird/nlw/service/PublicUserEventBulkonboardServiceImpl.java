package org.sunbird.nlw.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.profile.service.UserBulkUploadService;
import org.sunbird.storage.service.StorageServiceImpl;

import java.sql.Timestamp;
import java.util.*;

@Service
public class PublicUserEventBulkonboardServiceImpl implements PublicUserEventBulkonboardService {

    private Logger logger = LoggerFactory.getLogger(UserBulkUploadService.class);

    @Autowired
    StorageServiceImpl storageService;

    @Autowired
    CbExtServerProperties serverConfig;

    @Autowired
    Producer kafkaProducer;

    @Autowired
    CassandraOperation cassandraOperation;

    @Override
    public SBApiResponse bulkOnboard(MultipartFile mFile, String eventId, String batchId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.PUBLIC_USER_EVENT_BULKONBOARD);
        try {

            String errMsg = validateEventDetailsAndCSVFile(eventId, batchId, mFile);
            if (StringUtils.isNotEmpty(errMsg)) {
                setErrorData(response, errMsg);
                return response;
            }

            if (isFileExistForProcessing(eventId)) {
                setErrorData(response, "Failed to upload for another request as previous request is in processing state, please try after some time.");
                return response;
            }

            SBApiResponse uploadResponse = storageService.uploadFile(mFile, serverConfig.getEventBulkOnboardContainerName());
            if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
                setErrorData(response, String.format("Failed to upload file. Error: %s",
                        uploadResponse.getParams().getErrmsg()));
                return response;
            }

            Map<String, Object> uploadedFile = new HashMap<>();
            uploadedFile.put(Constants.CONTEXT_ID_CAMEL, eventId);
            uploadedFile.put(Constants.IDENTIFIER, UUID.randomUUID().toString());
            uploadedFile.put(Constants.FILE_NAME, uploadResponse.getResult().get(Constants.NAME));
            uploadedFile.put(Constants.FILE_PATH, uploadResponse.getResult().get(Constants.URL));
            uploadedFile.put(Constants.DATE_CREATED_ON, new Timestamp(System.currentTimeMillis()));
            uploadedFile.put(Constants.STATUS, Constants.INITIATED_CAPITAL);

            SBApiResponse insertResponse = cassandraOperation.insertRecord(Constants.SUNBIRD_KEY_SPACE_NAME,
                    serverConfig.getPublicUserEventBulkOnboardTable(), uploadedFile);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                setErrorData(response, "Failed to update database with event user bulk onboard file details.");
                return response;
            }

            uploadedFile.put(Constants.EVENT_ID, eventId);
            uploadedFile.put(Constants.BATCH_ID, batchId);
            kafkaProducer.push(serverConfig.getPublicUserEventBulkOnboardTopic(), uploadedFile);

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().putAll(uploadedFile);
        } catch (Exception e) {
            setErrorData(response,
                    String.format("Failed to process event user bulk onboard request. Error: ", e.getMessage()));
        }
        return response;
    }

    private void setErrorData(SBApiResponse response, String errMsg) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private boolean isFileExistForProcessing(String eventId) {
        Map<String, Object> bulkUplaodPrimaryKey = new HashMap<String, Object>();
        bulkUplaodPrimaryKey.put(Constants.PUBLIC_CONTEXT_ID, eventId);
        List<String> fields = Arrays.asList(Constants.PUBLIC_CONTEXT_ID, Constants.IDENTIFIER, Constants.STATUS);

        List<Map<String, Object>> bulkUploadMdoList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                Constants.KEYSPACE_SUNBIRD, serverConfig.getPublicUserEventBulkOnboardTable(), bulkUplaodPrimaryKey, fields);
        if (CollectionUtils.isEmpty(bulkUploadMdoList)) {
            return false;
        }
        return bulkUploadMdoList.stream()
                .anyMatch(entry -> Constants.STATUS_IN_PROGRESS_UPPERCASE.equalsIgnoreCase((String) entry.get(Constants.STATUS)));
    }

    private String validateEventDetailsAndCSVFile(String eventId, String batchId, MultipartFile mFile) {
        String errMsg;
        // Validate event details first
        errMsg = validateEventDetails(eventId, batchId);
        if (StringUtils.isNotEmpty(errMsg)) {
            logger.error("Validation failed for event details: {}", errMsg);
            return errMsg;
        }
        // Validate the CSV file next
        errMsg = validateCsvFile(mFile);
        if (StringUtils.isNotEmpty(errMsg)) {
            logger.error("Validation failed for CSV file: {}", errMsg);
            return errMsg;
        }
        return errMsg;
    }


    private String validateEventDetails(String eventId, String batchId) {
        String errMsg = "";
        logger.debug("Fetching event batch details for eventId: {} and batchId: {}", eventId, batchId);
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.EVENT_ID, eventId);
        propertiesMap.put(Constants.BATCH_ID, batchId);

        try {
            List<Map<String, Object>> eventBatchDetails = cassandraOperation.getRecordsByProperties(
                    Constants.SUNBIRD_COURSES_KEY_SPACE_NAME,
                    Constants.EVENT_BATCH_TABLE_NAME,
                    propertiesMap,
                    null
            );

            if (CollectionUtils.isEmpty(eventBatchDetails)) {
                errMsg = String.format("No event batch details found for eventId: %s and batchId: %s", eventId, batchId);
                logger.error(errMsg);
                return errMsg;
            }
        } catch (Exception e) {
            errMsg = String.format("Error while fetching event batch details for eventId: %s and batchId: %s", eventId, batchId);
            logger.error(errMsg, e);
            return errMsg;
        }

        return errMsg;
    }

    public String validateCsvFile(MultipartFile file) {
        String errMsg = "";
        // Check if the file is not null and not empty
        if (Objects.isNull(file) || file.isEmpty()) {
            errMsg = "File is empty or not provided.";
            return errMsg;
        }
        // Extract the file name and extension
        String fileName = file.getOriginalFilename();
        if (Objects.isNull(fileName)) {
            errMsg = "File name is invalid.";
            return errMsg;
        }
        // Validate the extension
        String extension = FilenameUtils.getExtension(fileName);
        if (!"csv".equalsIgnoreCase(extension)) {
            errMsg = "Invalid file type. Only CSV files are allowed.";
            return errMsg;
        }
        return errMsg;
    }

}
