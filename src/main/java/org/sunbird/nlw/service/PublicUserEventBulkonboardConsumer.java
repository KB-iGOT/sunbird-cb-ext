package org.sunbird.nlw.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.model.SunbirdApiRequest;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.storage.service.StorageService;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Component
public class PublicUserEventBulkonboardConsumer {

    private Logger logger = LoggerFactory.getLogger(PublicUserEventBulkonboardConsumer.class);

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    StorageService storageService;

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    CertificateServiceImpl certificateService;

    @Autowired
    ClaimEventKarmaPointsServiceImpl karmaPointsService;


    @KafkaListener(topics = "${public.user.event.bulk.onboard.topic}", groupId = "${public.user.event.bulk.onboard.topic.group}")
    public void processPublicUserEventBulkOnboardMessage(ConsumerRecord<String, String> data) {
        logger.info(
                "PublicUserEventBulkOnboardConsumer::processMessage: Received event to initiate Public user event Bulk Onboard Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    try {
                        initiatePublicUserEventBulkOnboardProcess(data.value());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                logger.error("Error in Public User Event Bulk Onboard Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Public User Event Bulk Onboard Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }


    public void initiatePublicUserEventBulkOnboardProcess(String inputData) throws IOException {
        logger.info("PublicUserEventBulkOnboardProcess:: initiatePublicUserEventBulkOnboardProcess: Started");
        long startTime = System.currentTimeMillis();
        Map<String, String> inputDataMap = objectMapper.readValue(inputData,
                new TypeReference<HashMap<String, String>>() {
                });

        List<String> errList = validateReceivedKafkaMessage(inputDataMap);
        if (errList.isEmpty()) {
            updateNLWUserBulkOnboardStatus(inputDataMap.get(Constants.CONTEXT_ID_CAMEL),
                    inputDataMap.get(Constants.IDENTIFIER), Constants.STATUS_IN_PROGRESS_UPPERCASE, 0, 0, 0);
            String fileName = inputDataMap.get(Constants.FILE_NAME);
            logger.info("fileName {} ", fileName);
            storageService.downloadFile(fileName, serverProperties.getEventBulkOnboardContainerName());
            processPublicUserEventBulkOnboard(inputDataMap);
        } else {
            logger.error(String.format("Error in the Kafka Message Received : %s", errList));
        }
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        logger.info("Total time taken to process public user event bulkonboard : " + totalTime);

    }

    private void processPublicUserEventBulkOnboard(Map<String, String> inputData) throws IOException {
        String eventId = inputData.get(Constants.EVENT_ID);
        String batchId = inputData.get(Constants.BATCH_ID);
        String status = "";

        int totalRecordsCount = 0;
        int processedCount = 0;
        int failedCount = 0;
        Map<String, String> emailUserIdMap = new HashMap<>();
        Map<String, Object> eventDetails = new HashMap<>();
        String columnName = "Email";

        File file = new File(Constants.LOCAL_BASE_PATH + inputData.get(Constants.FILE_NAME));
        if (!file.exists() || file.length() == 0) {
            logger.info("File not downloaded/present.");
            status = Constants.FAILED_UPPERCASE;
            updateStatus(inputData, status, totalRecordsCount, processedCount, failedCount);
            return;
        }

        List<Map<String, String>> updatedRecords = new ArrayList<>();
        List<String> headers;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(reader, CSVFormat.newFormat(serverProperties.getCsvDelimiter()).withFirstRecordAsHeader())) {

            headers = new ArrayList<>(csvParser.getHeaderNames());
            cleanHeaders(headers);

            if (!headers.contains("Status")) headers.add("Status");
            if (!headers.contains("Error Details")) headers.add("Error Details");

            int expectedFieldCount = headers.size() - 2; // Exclude "Status" and "Error Details"


            getUserIdList(csvParser, columnName, emailUserIdMap);
            getEventDetails(eventId, batchId, eventDetails);

            try (CSVParser csvParser2 = new CSVParser(new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)),
                    CSVFormat.newFormat(serverProperties.getCsvDelimiter()).withFirstRecordAsHeader())) {
                for (CSVRecord record : csvParser2.getRecords()) {
                    totalRecordsCount++;
                    Map<String, String> updatedRecord = processRecord(record, expectedFieldCount, eventId, batchId, emailUserIdMap, eventDetails);
                    updatedRecords.add(updatedRecord);

                    if ("FAILED".equalsIgnoreCase(updatedRecord.get("Status"))) {
                        failedCount++;
                    } else {
                        processedCount++;
                    }
                }
            }

            writeUpdatedCSV(file, headers, updatedRecords);
            status = finalizeStatus(totalRecordsCount, processedCount, failedCount, file);

        } catch (IOException e) {
            logger.error("Error processing file: ", e);
            status = Constants.FAILED_UPPERCASE;
        }

        updateStatus(inputData, status, totalRecordsCount, processedCount, failedCount);
    }

    private void getUserIdList(CSVParser csvParser, String columnName, Map<String, String> emailUserIdMap) throws IOException {
        if (csvParser == null) {
            throw new IllegalArgumentException("Invalid input: CSV parser must not be null or empty.");
        }

        List<String> emailList = new ArrayList<>();
        try {
            for (CSVRecord record : csvParser) {
                // Validate if the record contains the required column
                if (record.isMapped(columnName)) {
                    String columnValue = record.get(columnName);
                    if (StringUtils.isNotBlank(columnValue)) {
                        emailList.add(columnValue.trim());
                    } else {
                        logger.warn("Skipping empty value for column {} in record {}", columnName, record.getRecordNumber());
                    }
                } else {
                    logger.error("Column '{}' not found in CSV file.", columnName);
                    throw new IllegalArgumentException("CSV file does not contain the specified column: " + columnName);
                }
            }
            logger.debug("Fetched {} email addresses from CSV.", emailList.size());

            if (!emailList.isEmpty()) {
                emailUserIdMap.putAll(getUserId(Constants.EMAIL, emailList));
                logger.debug("User IDs successfully retrieved for the email list.");
            } else {
                logger.warn("No valid email addresses found in the CSV.");
            }
        } catch (Exception e) {
            logger.error("Error processing CSV file to fetch user IDs", e);
            throw new IOException("Failed to process CSV file", e);
        }
    }


    /**
     * Cleans up the headers by removing any surrounding quotes.
     */
    private void cleanHeaders(List<String> headers) {
        headers.replaceAll(header -> header.replaceAll("^\"|\"$", ""));
    }

    /**
     * Processes a single CSV record. Returns the updated record with status and error details.
     */
    private Map<String, String> processRecord(CSVRecord record, int expectedFieldCount, String eventId, String batchId, Map<String, String> emailUserIdMap, Map<String, Object> eventDetails) throws IOException {
        Map<String, String> updatedRecord = new LinkedHashMap<>(record.toMap());

        if (record.size() > expectedFieldCount) {
            markRecordAsFailed(updatedRecord, "Number of fields in the record exceeds expected number. Please check your data.");
            return updatedRecord;
        }

        String email = record.get("Email");
        if (StringUtils.isBlank(email)) {
            markRecordAsFailed(updatedRecord, "Empty email");
            return updatedRecord;
        }

        String userId = emailUserIdMap.get(email);
        if (StringUtils.isEmpty(userId)) {
            markRecordAsFailed(updatedRecord, "User does not exist");
            return updatedRecord;
        }

        if (isEventEnrolmentExist(userId, eventId, batchId)) {
            markRecordAsFailed(updatedRecord, "Already enrolled in the event");
            return updatedRecord;
        }

        SBApiResponse enrollmentResponse = enrollNLWEvent(userId, eventId, batchId, eventDetails);
        if (!Constants.SUCCESS.equalsIgnoreCase((String) enrollmentResponse.get(Constants.RESPONSE))) {
            markRecordAsFailed(updatedRecord, "Failed to enroll");
            return updatedRecord;
        }

        double completionPercentage = 100.0;
        certificateService.generateCertificateEventAndPushToKafka(userId, eventId, batchId, completionPercentage, eventDetails);
        karmaPointsService.generateKarmaPointEventAndPushToKafka(userId, eventId, batchId, eventDetails);
        logger.info("Successfully enrolled user: userId = {}, email = {}", userId, email);
        return updatedRecord;
    }

    /**
     * Marks a record as failed with an error message.
     */
    private void markRecordAsFailed(Map<String, String> record, String errorMessage) {
        record.put("Status", "FAILED");
        record.put("Error Details", errorMessage);
    }

    /**
     * Writes the updated records to the CSV file.
     */
    private void writeUpdatedCSV(File file, List<String> headers, List<Map<String, String>> updatedRecords) throws IOException {
        try (FileWriter fileWriter = new FileWriter(file);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
             CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.newFormat(serverProperties.getCsvDelimiter())
                     .withHeader(headers.toArray(new String[0]))
                     .withRecordSeparator(System.lineSeparator()))) {

            for (Map<String, String> record : updatedRecords) {
                csvPrinter.printRecord(record.values());
            }
        }
    }

    /**
     * Finalizes the status based on the record processing result.
     */
    private String finalizeStatus(int totalRecordsCount, int processedCount, int failedCount, File file) throws IOException {
        String status = uploadTheUpdatedCSVFile(file);
        if (Constants.SUCCESSFUL.equalsIgnoreCase(status) && failedCount == 0 && totalRecordsCount == processedCount && totalRecordsCount >= 1) {
            return Constants.SUCCESSFUL;
        }
        return Constants.FAILED_UPPERCASE;
    }

    /**
     * Updates the bulk onboarding status.
     */
    private void updateStatus(Map<String, String> inputData, String status, int totalRecordsCount, int processedCount, int failedCount) {
        updateNLWUserBulkOnboardStatus(inputData.get(Constants.CONTEXT_ID_CAMEL), inputData.get(Constants.IDENTIFIER), status, totalRecordsCount, processedCount, failedCount);
    }


    private Map<String, String> getUserId(String key, List<String> values) {
        int batchSize = 100;
        SunbirdApiRequest requestObj = new SunbirdApiRequest();
        List<Map<String, Object>> contentList = new ArrayList<>();
        Map<String, Object> reqMap = new HashMap<>();

        Map<String, String> emailUserIdMap = new HashMap<>();

        String url = serverProperties.getSbUrl() + serverProperties.getUserSearchEndPoint();
        HashMap<String, String> headersValue = new HashMap<>();
        headersValue.put(Constants.CONTENT_TYPE, "application/json");
        headersValue.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());

        for (int i = 0; i < values.size(); i += batchSize) {
            int end = Math.min(i + batchSize, values.size());
            List<String> subList = values.subList(i, end);
            reqMap.put(Constants.FILTERS, new HashMap<String, Object>() {
                {
                    put(key, subList);
                }
            });
            requestObj.setRequest(reqMap);

            try {
                Map<String, Object> response = outboundRequestHandlerService.fetchResultUsingPost(url, requestObj,
                        headersValue);
                if (response != null && "OK".equalsIgnoreCase((String) response.get("responseCode"))) {
                    Map<String, Object> map = (Map<String, Object>) response.get("result");
                    if (map.get("response") != null) {
                        Map<String, Object> responseObj = (Map<String, Object>) map.get("response");
                        contentList = (List<Map<String, Object>>) responseObj.get(Constants.CONTENT);
                        if (contentList != null) {
                            contentList.stream()
                                    .forEach(e -> {
                                        Map<String, Object> profileDetails = (Map<String, Object>) e.get(Constants.PROFILE_DETAILS);
                                        if (profileDetails != null) {
                                            Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
                                            if (personalDetails != null) {
                                                emailUserIdMap.put(
                                                        (String) personalDetails.get(Constants.PRIMARY_EMAIL),
                                                        (String) e.get(Constants.IDENTIFIER)
                                                );
                                            }
                                        }
                                    });

                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error while fetching user details of list of users ", e);
            }
        }
        return emailUserIdMap;
    }

    private SBApiResponse enrollNLWEvent(String userId, String eventId, String batchId, Map<String, Object> eventDetails) throws JsonProcessingException {

        int defaultStatus = 2;
        int defaultProgress = 100;
        float defaultCompletionPercentage = 100;

        Map<String, Object> request = new HashMap<>();
        request.put(Constants.USER_ID, userId);
        request.put(Constants.CONTENT_ID_KEY, eventId);
        request.put(Constants.CONTEXT_ID_CAMEL, eventId);
        request.put(Constants.BATCH_ID, batchId);
        request.put(Constants.ACTIVE, true);
        request.put(Constants.STATUS, defaultStatus);
        request.put(Constants.PROGRESS, defaultProgress);
        request.put(Constants.COMPLETION_PERCENTAGE, defaultCompletionPercentage);
        request.put(Constants.ENROLLED_DATE_KEY_LOWER, eventDetails.get(Constants.START_DATE));
        request.put(Constants.DATE_TIME, eventDetails.get(Constants.START_DATE));
        request.put(Constants.COMPLETED_ON, eventDetails.get(Constants.END_DATE));
        request.put(Constants.LRC_PROGRESS_DETAILS_COLUMN, prepareLrcProgressDetails(eventDetails));

        logger.info("Attempting to enroll user: userId = {}, eventId = {}, batchId = {}", userId, eventId, batchId);

        SBApiResponse response;
        try {

            response = cassandraOperation.insertRecord(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, serverProperties.getUserEventEnrolmentTable(), request);

        } catch (Exception e) {
            logger.error("Exception while enrolling user: userId = {}, eventId = {}, batchId = {}", userId, eventId, batchId, e);
            response = ProjectUtil.createDefaultResponse(Constants.PUBLIC_USER_EVENT_BULKONBOARD);
            response.put(Constants.RESPONSE, Constants.FAILED);
        }

        return response;
    }

    private boolean isEventEnrolmentExist(String userId, String eventId, String batchId) {

        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.USER_ID, userId);
        compositeKey.put(Constants.CONTENT_ID_KEY, eventId);
        compositeKey.put(Constants.CONTEXT_ID_CAMEL, eventId);
        compositeKey.put(Constants.BATCH_ID, batchId);

        List<Map<String, Object>> enrolmentRecords = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, serverProperties.getUserEventEnrolmentTable(), compositeKey, null);
        return CollectionUtils.isNotEmpty(enrolmentRecords);
    }

    private List<String> validateReceivedKafkaMessage(Map<String, String> inputDataMap) {
        StringBuffer str = new StringBuffer();
        List<String> errList = new ArrayList<>();
        if (StringUtils.isEmpty(inputDataMap.get(Constants.EVENT_ID))) {
            errList.add("Event ID is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.BATCH_ID))) {
            errList.add("Batch ID is not present");
        }
        if (org.apache.commons.lang.StringUtils.isEmpty(inputDataMap.get(Constants.FILE_NAME))) {
            errList.add("Filename is not present");
        }
        if (!errList.isEmpty()) {
            str.append("Failed to Validate event Details. Error Details - [").append(errList.toString()).append("]");
        }
        return errList;
    }

    private String uploadTheUpdatedCSVFile(File file) throws IOException {
        SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getEventBulkOnboardContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.info(String.format("Failed to upload file. Error: %s",
                    uploadResponse.getParams().getErrmsg()));
            return Constants.FAILED_UPPERCASE;
        }
        return Constants.SUCCESSFUL_UPPERCASE;
    }

    public void updateNLWUserBulkOnboardStatus(String contextId, String identifier, String status, int totalRecordsCount,
                                               int successfulRecordsCount, int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.CONTEXT_ID_CAMEL, contextId);
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
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, serverProperties.getPublicUserEventBulkOnboardTable(),
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error(String.format("Error in Updating User Bulk Upload Status in Cassandra %s", e.getMessage()), e);
        }
    }

    private void getEventDetails(String eventId, String batchId, Map<String, Object> eventDetails) {
        logger.debug("Fetching event batch details for eventId: {} and batchId: {}", eventId, batchId);
        Map<String, Object> propertiesMap = new HashMap<>();
        propertiesMap.put(Constants.EVENT_ID, eventId);
        propertiesMap.put(Constants.BATCH_ID, batchId);
        try {
            List<Map<String, Object>> eventBatchDetails = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.EVENT_BATCH_TABLE_NAME, propertiesMap, null);
            if (CollectionUtils.isNotEmpty(eventBatchDetails)) {
                Map<String, Object> eventBatch = eventBatchDetails.get(0);
                String batchAttributesStr = (String) eventBatch.get(Constants.BATCH_ATTRIBUTES_COLUMN);
                Map<String, Object> batchAttributes = objectMapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {
                });
                eventDetails.put(Constants.START_DATE, prepareEventDateTime((Date) eventBatch.get(Constants.START_DATE_COLUMN), (String) batchAttributes.get(Constants.START_TIME_KEY)));
                eventDetails.put(Constants.END_DATE, prepareEventDateTime((Date) eventBatch.get(Constants.END_DATE_COLUMN), (String) batchAttributes.get(Constants.END_TIME_KEY)));

                long durationInSec = ((long) batchAttributes.get(Constants.DURATION)) * 60;
                eventDetails.put(Constants.DURATION, durationInSec);
            } else {
                logger.warn("No event batch details found for eventId: {} and batchId: {}", eventId, batchId);
            }
        } catch (Exception e) {
            logger.error("Error while fetching event batch details for eventId: {} and batchId: {}", eventId, batchId, e);
            throw new RuntimeException("Unable to fetch event details", e);
        }
    }

    private Date prepareEventDateTime(Date date, String time) {
        if (date == null || time == null || time.isEmpty()) {
            throw new IllegalArgumentException("Date and time must not be null or empty.");
        }
        LocalDateTime localDateTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ssXXXXX");
        OffsetTime timePart;

        try {
            timePart = OffsetTime.parse(time, timeFormatter);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid time format: " + time, e);
        }
        ZonedDateTime combinedDateTime = localDateTime.atZone(ZoneId.systemDefault())
                .withHour(timePart.getHour())
                .withMinute(timePart.getMinute())
                .withSecond(timePart.getSecond())
                .withNano(timePart.getNano())
                .withZoneSameInstant(timePart.getOffset());

        Timestamp resultTimestamp = Timestamp.from(combinedDateTime.toInstant());
        return new Date(resultTimestamp.getTime()); // Convert Timestamp to Date
    }

    private String prepareLrcProgressDetails(Map<String, Object> eventDetails) throws JsonProcessingException {
        Map<String, Object> result = new HashMap<>();
        result.put("max_size", eventDetails.get(Constants.DURATION));
        result.put("duration", eventDetails.get(Constants.DURATION));
        result.put("mimeType", "application/html");
        result.put("stateMetaData", eventDetails.get(Constants.DURATION));
        result.put("current", Arrays.asList(0));

        return objectMapper.writeValueAsString(result);
    }

}
