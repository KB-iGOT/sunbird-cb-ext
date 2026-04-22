package org.sunbird.peervalidation.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.IndexerService;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.storage.service.StorageService;

import java.io.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.PreDestroy;

@Component
public class PeerValidationReportConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PeerValidationReportConsumer.class);

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    private ObjectMapper mapper;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private IndexerService indexerService;

    @Autowired
    private StorageService storageService;

    private static final String CONSUMER_IDENTITY = "user-survey-report-consumer";
    private static final int PAGE_SIZE = 100;

    private final List<String> messageBuffer = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(topics = "${kafka.topics.report.download.requests}", groupId = "${kafka.topics.report.download.requests.group}")
    public void processReportDownloadRequest(ConsumerRecord<String, String> data) {
        try {
            logger.info("PeerValidationReportConsumer::processReportDownloadRequest: topic name: {} and receivedData: {}", data.topic(), data.value());
            String message = data.value();

            messageBuffer.add(message);

            Map<String, Object> requestMap = mapper.readValue(message, new TypeReference<Map<String, Object>>() {});
            CompletableFuture.runAsync(() -> {
                try {
                    initiateReportGeneration(requestMap);
                } finally {
                    // Remove from buffer after processing (success or failure)
                    messageBuffer.remove(message);
                }
            });
        } catch (Exception e) {
            logger.error("Error while processing the kafka event : " + data, e);
            // Remove from buffer if exception occurs before async processing
            messageBuffer.remove(data.value());
        }
    }

    /**
     * Shutdown hook to handle graceful shutdown.
     * Updates any in-progress report generations to FAILED status.
     */
    @PreDestroy
    public void shutdownHook() {
        logger.info("Shutdown hook triggered. Processing buffered messages for PeerValidationReportConsumer...");

        synchronized (messageBuffer) {
            if (messageBuffer.isEmpty()) {
                logger.info("No buffered messages to process during shutdown.");
                return;
            }

            logger.warn("Found {} in-progress report generation requests during shutdown. Marking as FAILED.", messageBuffer.size());

            for (String message : messageBuffer) {
                try {
                    logger.info("Processing buffered message during shutdown: {}", message);
                    updateDBStatusAtShutDown(message);
                } catch (Exception e) {
                    logger.error("Error processing buffered message during shutdown: {}", message, e);
                }
            }

            messageBuffer.clear();
            logger.info("Shutdown hook completed. All buffered messages processed.");
        }
    }

    /**
     * Update database status to FAILED for reports interrupted during shutdown
     */
    private void updateDBStatusAtShutDown(String message) {
        try {
            Map<String, Object> requestMap = mapper.readValue(message, new TypeReference<Map<String, Object>>() {});

            String rootOrgId = (String) requestMap.get(Constants.ROOT_ORG_ID);
            String formId = (String) requestMap.get(Constants.FORM_ID);
            String identifier = (String) requestMap.get(Constants.IDENTIFIER);

            if (StringUtils.isBlank(rootOrgId) || StringUtils.isBlank(formId) || StringUtils.isBlank(identifier)) {
                logger.warn("Invalid message during shutdown. Missing required fields: {}", message);
                return;
            }

            String errorMessage = "Report generation interrupted during application shutdown";

            updateCassandraStatus(rootOrgId, formId, identifier, Constants.FAILED_UPPERCASE,
                    errorMessage, 0, 0, 0, null, null, null,
                    null);
            logger.info("Updated status to FAILED for report generation interrupted during shutdown. Identifier: {}", identifier);

        } catch (Exception e) {
            logger.error("Failed to update status during shutdown for message: {}", message, e);
        }
    }

    public void initiateReportGeneration(Map<String, Object> requestMap) {
        try {
            logger.info("PeerValidationReportConsumer::initiateReportGeneration: Started for request: {}", requestMap);

            // Parse and validate payload
            String rootOrgId = (String) requestMap.get(Constants.ROOT_ORG_ID);
            String formId = (String) requestMap.get(Constants.FORM_ID);
            String identifier = (String) requestMap.get(Constants.IDENTIFIER);
            String requestedBy = (String) requestMap.get(Constants.REQUESTED_BY_CAMEL);

            if (StringUtils.isBlank(rootOrgId) || StringUtils.isBlank(formId) ||
                StringUtils.isBlank(identifier) || StringUtils.isBlank(requestedBy)) {
                logger.error("Invalid Kafka message payload. Missing required fields. rootOrgId: {}, formId: {}, identifier: {}, requestedBy: {}",
                        rootOrgId, formId, identifier, requestedBy);
                return;
            }

            logger.info("Processing report generation. rootOrgId: {}, formId: {}, identifier: {}",
                    rootOrgId, formId, identifier);

            // Fetch questions from ElasticSearch and sort by order
            List<Map<String, Object>> questions = fetchQuestions(formId);
            if (questions.isEmpty()) {
                String errorMsg = "No questions found for formId: " + formId;
                logger.error(errorMsg);
                updateCassandraStatus(rootOrgId, formId, identifier, Constants.FAILED_UPPERCASE,
                        errorMsg, 0, 0, 0, null, null, null, requestedBy);
                return;
            }

            logger.info("Fetched {} questions for formId: {}", questions.size(), formId);

            // Generate CSV file
            File csvFile = generateCSVReport(formId, identifier, questions);

            // Count total records processed
            int totalRecords = countLinesInFile(csvFile) - 1; // Subtract header row

            String objectKey = serverProperties.getPeerValidationCloudFolderName() + Constants.SEPARATOR_SLASH
                    + rootOrgId + Constants.SEPARATOR_SLASH
                    + formId;

            // Upload CSV to storage (using regular upload for now, TTL to be tested separately)
            SBApiResponse uploadResponse = storageService.uploadFile(csvFile,
                    objectKey,
                    serverProperties.getCloudPublicContainerName());

            if (uploadResponse == null || !Constants.SUCCESS.equalsIgnoreCase(uploadResponse.getParams().getStatus())) {
                String errorMsg = "Failed to upload CSV file to storage";
                logger.error(errorMsg);
                updateCassandraStatus(rootOrgId, formId, identifier, Constants.FAILED_UPPERCASE,
                        errorMsg, totalRecords, totalRecords, 0, null, null, null, requestedBy);
                return;
            }

            // Extract artifact URL and filename from upload response
            Map<String, Object> uploadResult = uploadResponse.getResult();
            String artifactUrl = (String) uploadResult.get(Constants.URL);
            String fileName = (String) uploadResult.get(Constants.NAME);

            logger.info("Successfully uploaded CSV file. ArtifactUrl: {}, FileName: {}", artifactUrl, fileName);

            // Update Cassandra with success status
            updateCassandraStatus(rootOrgId, formId, identifier, Constants.COMPLETED_STATUS,
                    null, totalRecords, totalRecords, 0, fileName, null, artifactUrl, requestedBy);

            logger.info("Report generation completed successfully for identifier: {}", identifier);

        } catch (Exception e) {
            logger.error("Report generation failed for request: {}", requestMap, e);
            updateCassandraStatus((String) requestMap.get(Constants.ROOT_ORG_ID), (String) requestMap.get(Constants.FORM_ID),
                    (String) requestMap.get(Constants.IDENTIFIER), Constants.FAILED_UPPERCASE, e.getMessage(), 0, 0, 0, null, null, null, (String) requestMap.get(Constants.REQUESTED_BY_CAMEL));
        }
    }

    /**
     * Fetch questions from ElasticSearch
     */
    private List<Map<String, Object>> fetchQuestions(String formId) throws IOException {
        List<Map<String, Object>> questions = new ArrayList<>();

        BoolQueryBuilder query = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery(Constants.CONTEXT_TYPE, Constants.QUESTION))
                .must(QueryBuilders.termQuery(Constants.FORM_ID, formId));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        sourceBuilder.size(100); // Assuming max 100 questions per form

        SearchResponse searchResponse = indexerService.getEsResult(
                serverProperties.getFormMetaDataIndex(),
                Constants.ES_DOC_TYPE,
                sourceBuilder,
                ProjectUtil.ESIndexType.IGOT_ES
        );

        if (searchResponse != null && searchResponse.getHits().getTotalHits() > 0) {
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                Map<String, Object> source = hit.getSourceAsMap();

                Map<String, Object> questionInfo = new HashMap<>();
                questionInfo.put(Constants.SUBMISSION_QUESTION_ID, hit.getId()); // Use document _id as questionId
                questionInfo.put(Constants.NAME, source.get(Constants.NAME));
                questionInfo.put(Constants.ORDER, source.get(Constants.ORDER) != null ?
                        ((Number) source.get(Constants.ORDER)).intValue() : 0);

                questions.add(questionInfo);
            }
        }
        // Sort by order ascending
        questions.sort(Comparator.comparingInt(q -> (int) q.get(Constants.ORDER)));

        logger.info("Fetched {} questions for formId: {}", questions.size(), formId);
        return questions;
    }

    /**
     * Generate CSV report with streaming
     */
    private File generateCSVReport(String formId, String identifier, List<Map<String, Object>> questions) throws IOException {

        // Generate filename with date and time
        SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.DATE_FORMAT_YYYYMMDD_HHMMSS);
        File csvFile = new File(System.getProperty("java.io.tmpdir"),
                Constants.REPORT_FILE_PREFIX + formId + Constants.UNDERSCORE + dateFormat.format(new Date()) + Constants.CSV_FILE);

        logger.info("Generating CSV report at: {}", csvFile.getAbsolutePath());

        try (FileWriter fileWriter = new FileWriter(csvFile);
             BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
             CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter,
                     CSVFormat.DEFAULT)) {

            // Write header row
            List<String> headers = buildHeaders(questions);
            csvPrinter.printRecord(headers);

            // Build question ID to index mapping for fast lookup
            // Column indices: 0=FullName, 1..N=Questions, N+1..N+6=Peers
            Map<String, Integer> questionIdToColumnIndex = new HashMap<>();
            for (int i = 0; i < questions.size(); i++) {
                questionIdToColumnIndex.put((String) questions.get(i).get(Constants.SUBMISSION_QUESTION_ID), i + 1); // +1 for full name column
            }

            // First pass: collect all submissions and peer IDs
            List<Map<String, Object>> allSubmissions = new ArrayList<>();
            Set<String> allPeerIds = new HashSet<>();
            Object[] searchAfter = null;

            logger.info("Starting first pass to collect submissions and peer IDs for formId: {}", formId);

            while (true) {
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

                BoolQueryBuilder query = QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(Constants.FORM_ID, formId))
                        .must(QueryBuilders.termQuery(Constants.CONTEXT_TYPE, Constants.CONTEXT_TYPE_PEER_VALIDATION_SURVEY))
                        .must(QueryBuilders.termQuery(Constants.STATUS, Constants.SUBMITTED));

                sourceBuilder.query(query);
                sourceBuilder.size(PAGE_SIZE);
                // Use _id for sorting as it's always available and sortable
                sourceBuilder.sort(Constants.ES_SORT_FIELD_ID, SortOrder.ASC);

                if (searchAfter != null) {
                    sourceBuilder.searchAfter(searchAfter);
                }

                SearchResponse searchResponse = indexerService.getEsResult(
                        serverProperties.getUserFormDataIndexV2(),
                        Constants.ES_DOC_TYPE,
                        sourceBuilder,
                        ProjectUtil.ESIndexType.IGOT_ES
                );

                SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits == null || hits.length == 0) {
                    break; // No more results
                }

                // Collect submissions and extract peer IDs
                for (SearchHit hit : hits) {
                    Map<String, Object> submission = hit.getSourceAsMap();
                    allSubmissions.add(submission);

                    // Extract peer IDs from this submission
                    List<Map<String, Object>> peerReviews = (List<Map<String, Object>>) submission.get(Constants.PEER_REVIEWS);
                    if (peerReviews != null) {
                        for (Map<String, Object> peer : peerReviews) {
                            String peerId = getStringValue(peer.get(Constants.PEER_ID));
                            if (StringUtils.isNotBlank(peerId)) {
                                allPeerIds.add(peerId);
                            }
                        }
                    }
                }

                // Prepare for next page
                searchAfter = hits[hits.length - 1].getSortValues();
                logger.info("Collected {} submissions so far, total unique peer IDs: {}", allSubmissions.size(), allPeerIds.size());
            }

            logger.info("First pass complete. Total submissions: {}, Total unique peer IDs: {}", allSubmissions.size(), allPeerIds.size());

            // Bulk fetch user first names for all peer IDs
            Map<String, String> peerIdToFirstName = bulkFetchUserFirstNames(allPeerIds);
            logger.info("Bulk fetched {} user first names", peerIdToFirstName.size());

            // Second pass: write CSV rows using cached data
            int totalSubmissions = 0;
            for (Map<String, Object> submission : allSubmissions) {
                List<String> rowValues = buildRowValues(submission, questions, questionIdToColumnIndex, peerIdToFirstName);
                csvPrinter.printRecord(rowValues);
                totalSubmissions++;
            }

            logger.info("Total submissions written to CSV: {}", totalSubmissions);
        }
        return csvFile;
    }

    /**
     * Build CSV header row
     */
    private List<String> buildHeaders(List<Map<String, Object>> questions) {
        List<String> headers = new ArrayList<>();

        // Add Full Name as first column
        headers.add(Constants.CSV_HEADER_FULL_NAME);

        // Add Organisation name as second column
        headers.add(Constants.CSV_HEADER_ORGANISATION_NAME);

        // Add Submitted on as third column
        headers.add(Constants.CSV_HEADER_SUBMITTED_ON);

        // Add question headers (sorted by order)
        for (Map<String, Object> question : questions) {
            headers.add((String) question.get(Constants.NAME));
        }

        // Add attachment headers
        headers.add(Constants.CSV_HEADER_ATTACHED_VIDEO);
        headers.add(Constants.CSV_HEADER_ATTACHED_DOCUMENT);

        // Add peer review headers dynamically based on configuration
        int maxPeers = serverProperties.getPeerValidationReportMaxPeers();
        for (int i = 1; i <= maxPeers; i++) {
            headers.add(Constants.CSV_HEADER_PEER_PREFIX + i + Constants.PEER_NAME_DESIGNATION_SUFFIX);
            headers.add(Constants.CSV_HEADER_PEER_PREFIX + i + " " + Constants.CSV_HEADER_PEER_RESPONSE);
        }

        return headers;
    }

    /**
     * Build CSV row values for a submission
     */
    private List<String> buildRowValues(Map<String, Object> submission,
                                        List<Map<String, Object>> questions,
                                        Map<String, Integer> questionIdToColumnIndex,
                                        Map<String, String> peerIdToFirstName) {

        int maxPeers = serverProperties.getPeerValidationReportMaxPeers();
        int peerColumns = maxPeers * 2; // Each peer has 2 columns (Name+Designation and Response)

        // Initialize row with empty values (1 user column + 2 new columns + N question columns + 2 attachment columns + peer columns)
        String[] rowArray = new String[3 + questions.size() + 2 + peerColumns];
        Arrays.fill(rowArray, "");

        // Fill Full Name (column 0) - fetch from user index
        String submittedBy = getStringValue(submission.get(Constants.SUBMITTED_BY));
        String fullName = fetchUserFullName(submittedBy);
        rowArray[0] = StringUtils.isNotBlank(fullName) ? fullName : getStringValue(submission.get(Constants.USER_FULL_NAME));

        // Fill Organisation name (column 1)
        rowArray[1] = getStringValue(submission.get(Constants.ORG_NAME));

        // Fill Submitted on (column 2) - convert timestamp to readable format
        Object submittedDateObj = submission.get(Constants.SUBMITTED_DATE);
        if (submittedDateObj != null) {
            try {
                long timestamp = 0;
                if (submittedDateObj instanceof Number) {
                    timestamp = ((Number) submittedDateObj).longValue();
                } else if (submittedDateObj instanceof String) {
                    timestamp = Long.parseLong((String) submittedDateObj);
                }

                if (timestamp > 0) {
                    SimpleDateFormat sdf = new SimpleDateFormat(Constants.DATE_FORMAT_DD_MM_YYYY_HH_MM_SS);
                    rowArray[2] = sdf.format(new Date(timestamp));
                }
            } catch (Exception e) {
                logger.warn("Error parsing submitted date: {}", submittedDateObj, e);
                rowArray[2] = "";
            }
        }

        // Build questionId -> answer map from submission responses
        List<Map<String, Object>> responses = (List<Map<String, Object>>) submission.get(Constants.SUBMISSION_RESPONSES);
        if (responses != null) {
            for (Map<String, Object> response : responses) {
                String questionId = (String) response.get(Constants.SUBMISSION_QUESTION_ID);
                String answer = getStringValue(response.get(Constants.ANSWER));

                Integer columnIndex = questionIdToColumnIndex.get(questionId);
                if (columnIndex != null && columnIndex < rowArray.length) {
                    // Adjust column index by +2 for the new Organisation and Submitted on columns
                    int adjustedIndex = columnIndex + 2;
                    rowArray[adjustedIndex] = answer;
                }
            }
        }

        // Fill attachment columns (after questions, starting at index 3 + questions.size())
        int attachmentStartIndex = 3 + questions.size();
        List<Object> attachments = (List<Object>) submission.get(Constants.ATTACHMENTS);
        if (attachments != null && !attachments.isEmpty()) {
            for (Object attachment : attachments) {
                String attachmentUrl = getStringValue(attachment);
                if (StringUtils.isNotBlank(attachmentUrl)) {
                    String lowerUrl = attachmentUrl.toLowerCase();
                    if (lowerUrl.endsWith(Constants.FILE_EXTENSION_MP4)) {
                        rowArray[attachmentStartIndex] = attachmentUrl; // attached video
                    } else if (lowerUrl.endsWith(Constants.FILE_EXTENSION_PDF)) {
                        rowArray[attachmentStartIndex + 1] = attachmentUrl; // attached document
                    }
                }
            }
        }

        // Fill peer review columns (after attachments, starting at index 3 + questions.size() + 2)
        List<Map<String, Object>> peerReviews = (List<Map<String, Object>>) submission.get(Constants.PEER_REVIEWS);
        if (peerReviews != null) {
            int peerStartIndex = 3 + questions.size() + 2;
            for (int i = 0; i < Math.min(maxPeers, peerReviews.size()); i++) {
                Map<String, Object> peer = peerReviews.get(i);
                if (peer != null) {
                    int peerDetailsIndex = peerStartIndex + (i * 2);
                    int peerResponseIndex = peerStartIndex + (i * 2) + 1;

                    // Use cached firstName from bulk fetch
                    String peerId = getStringValue(peer.get(Constants.PEER_ID));
                    String peerFirstName = peerIdToFirstName.getOrDefault(peerId, peerId);
                    String designation = getStringValue(peer.get(Constants.DESIGNATION));

                    // Format: "Name, Designation"
                    String peerDetails = StringUtils.isNotBlank(peerFirstName) ? peerFirstName : peerId;
                    if (StringUtils.isNotBlank(designation)) {
                        peerDetails += ", " + designation;
                    }

                    rowArray[peerDetailsIndex] = peerDetails;
                    rowArray[peerResponseIndex] = getStringValue(peer.get(Constants.STATUS));
                }
            }
        }

        return Arrays.asList(rowArray);
    }

    /**
     * Convert object to string, handling nulls and collections
     */
    private String getStringValue(Object value) {
        if (value == null) {
            return "";
        }
        if (value instanceof String) {
            return (String) value;
        }
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            return list.stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(", "));
        }
        return value.toString();
    }

    /**
     * Update Cassandra status
     */
    private void updateCassandraStatus(String rootOrgId, String formId, String identifier,
                                      String status, String errorMessage,
                                      int totalRecords, int successfulCount, int failedCount,
                                      String fileName, String filePath, String artifactUrl, String updatedBy) {
        try {
            // Composite key for WHERE clause
            Map<String, Object> compositeKey = new HashMap<>();
            compositeKey.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            compositeKey.put(Constants.FORM_ID_LOWER, formId);
            compositeKey.put(Constants.IDENTIFIER, identifier);

            // Fields to update
            Map<String, Object> updateRecord = new HashMap<>();
            updateRecord.put(Constants.STATUS, status);
            updateRecord.put(Constants.DATE_UPDATED_ON_CASSANDRA, new Timestamp(System.currentTimeMillis()));
            updateRecord.put(Constants.UPDATED_BY, StringUtils.isNotBlank(updatedBy) ? updatedBy : CONSUMER_IDENTITY);
            updateRecord.put(Constants.TOTAL_RECORDS_CASSANDRA, totalRecords);
            updateRecord.put(Constants.SUCCESSFUL_RECORDS_COUNT_CASSANDRA, successfulCount);
            updateRecord.put(Constants.FAILED_RECORDS_COUNT_CASSANDRA, failedCount);

            if (StringUtils.isNotBlank(errorMessage)) {
                updateRecord.put(Constants.ERROR_MESSAGE_LOWER, errorMessage);
            }

            if (StringUtils.isNotBlank(fileName)) {
                updateRecord.put(Constants.FILE_NAME, fileName);
            }

            if (StringUtils.isNotBlank(filePath)) {
                updateRecord.put(Constants.FILE_PATH_LOWER, filePath);
            }

            if (StringUtils.isNotBlank(artifactUrl)) {
                updateRecord.put(Constants.ARTIFACT_URL, artifactUrl);
            }

            cassandraOperation.updateRecordWithTTL(Constants.KEYSPACE_SUNBIRD, Constants.USER_SURVEY_REPORT,
                    updateRecord, compositeKey, serverProperties.getPeerValidationReportTtlSeconds());
            logger.info("Updated Cassandra status to {} for identifier: {} with TTL: {} seconds",
                    status, identifier, serverProperties.getPeerValidationReportTtlSeconds());
        } catch (Exception e) {
            logger.error("Failed to update Cassandra status for identifier: {}", identifier, e);
        }
    }

    /**
     * Count lines in file
     */
    private int countLinesInFile(File file) {
        int lines = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            while (reader.readLine() != null) {
                lines++;
            }
        } catch (IOException e) {
            logger.error("Error counting lines in file: {}", file.getName(), e);
        }
        return lines;
    }

    /**
     * Bulk fetch user first names from user_alias index for multiple user IDs
     */
    private Map<String, String> bulkFetchUserFirstNames(Set<String> userIds) {
        Map<String, String> userIdToFirstName = new HashMap<>();

        if (userIds == null || userIds.isEmpty()) {
            logger.info("No user IDs to fetch");
            return userIdToFirstName;
        }

        try {
            // Convert Set to List for termsQuery
            List<String> userIdList = new ArrayList<>(userIds);

            // Process in batches of 1000 to avoid query size limits
            int batchSize = 1000;
            for (int i = 0; i < userIdList.size(); i += batchSize) {
                int end = Math.min(i + batchSize, userIdList.size());
                List<String> batch = userIdList.subList(i, end);

                BoolQueryBuilder query = QueryBuilders.boolQuery()
                        .must(QueryBuilders.termsQuery(Constants.IDENTIFIER, batch));

                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                sourceBuilder.query(query);
                sourceBuilder.size(batchSize);
                sourceBuilder.fetchSource(new String[]{Constants.IDENTIFIER, Constants.USER_FIRST_NAME}, null);

                SearchResponse searchResponse = indexerService.getEsResult(
                        serverProperties.getSbEsUserProfileIndex(),
                        serverProperties.getSbEsProfileIndexType(),
                        sourceBuilder,
                        ProjectUtil.ESIndexType.USER_ES
                );

                if (searchResponse != null && searchResponse.getHits().getTotalHits() > 0) {
                    for (SearchHit hit : searchResponse.getHits().getHits()) {
                        Map<String, Object> source = hit.getSourceAsMap();
                        String userId = getStringValue(source.get(Constants.IDENTIFIER));
                        String firstName = getStringValue(source.get(Constants.USER_FIRST_NAME));

                        if (StringUtils.isNotBlank(userId)) {
                            userIdToFirstName.put(userId, StringUtils.isNotBlank(firstName) ? firstName : "");
                        }
                    }
                }

                logger.info("Fetched {} user first names in batch {}", userIdToFirstName.size(), (i / batchSize) + 1);
            }

            logger.info("Bulk fetch complete. Total user first names fetched: {}", userIdToFirstName.size());
        } catch (Exception e) {
            logger.error("Error bulk fetching user first names", e);
        }
        return userIdToFirstName;
    }

    /**
     * Fetch user's full name from user_alias index by user ID
     */
    private String fetchUserFullName(String userId) {
        if (StringUtils.isBlank(userId)) {
            return "";
        }

        try {
            BoolQueryBuilder query = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(Constants.IDENTIFIER, userId));

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(query);
            sourceBuilder.size(1);
            sourceBuilder.fetchSource(new String[]{Constants.USER_FIRST_NAME}, null);

            SearchResponse searchResponse = indexerService.getEsResult(
                    serverProperties.getSbEsUserProfileIndex(),
                    serverProperties.getSbEsProfileIndexType(),
                    sourceBuilder,
                    ProjectUtil.ESIndexType.USER_ES
            );

            if (searchResponse != null && searchResponse.getHits().getTotalHits() > 0) {
                SearchHit hit = searchResponse.getHits().getHits()[0];
                Map<String, Object> source = hit.getSourceAsMap();
                String firstName = getStringValue(source.get(Constants.USER_FIRST_NAME));
                return StringUtils.isNotBlank(firstName) ? firstName : "";
            }
        } catch (Exception e) {
            logger.error("Error fetching user full name for userId: {}", userId, e);
        }
        return "";
    }
}
