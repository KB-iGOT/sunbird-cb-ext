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
    private static final int MAX_ERROR_MESSAGE_LENGTH = 1000;

    @KafkaListener(topics = "${kafka.topics.report.download.requests}", groupId = "${kafka.topics.report.download.requests.group}")
    public void processReportDownloadRequest(ConsumerRecord<String, String> data) {
        try {
            logger.info("PeerValidationReportConsumer::processReportDownloadRequest: topic name: {} and receivedData: {}", data.topic(), data.value());
            Map<String, Object> requestMap = mapper.readValue(data.value(), new TypeReference<Map<String, Object>>() {});
            CompletableFuture.runAsync(() -> initiateReportGeneration(requestMap));
        } catch (Exception e) {
            logger.error("Error while processing the kafka event : " + data);
        }
    }

    public void initiateReportGeneration(Map<String, Object> requestMap) {
        String rootOrgId = null;
        String formId = null;
        String identifier = null;
        String requestedBy = null;

        try {
            logger.info("PeerValidationReportConsumer::initiateReportGeneration: Started for request: {}", requestMap);

            // 1. Parse and validate payload
            rootOrgId = (String) requestMap.get(Constants.ROOT_ORG_ID);
            formId = (String) requestMap.get(Constants.FORM_ID);
            identifier = (String) requestMap.get(Constants.IDENTIFIER);
            requestedBy = (String) requestMap.get(Constants.REQUESTED_BY_CAMEL);

            if (StringUtils.isBlank(rootOrgId) || StringUtils.isBlank(formId) ||
                StringUtils.isBlank(identifier) || StringUtils.isBlank(requestedBy)) {
                logger.error("Invalid Kafka message payload. Missing required fields. rootOrgId: {}, formId: {}, identifier: {}, requestedBy: {}",
                        rootOrgId, formId, identifier, requestedBy);
                return;
            }

            logger.info("Processing report generation. rootOrgId: {}, formId: {}, identifier: {}",
                    rootOrgId, formId, identifier);

            // 2. Idempotency check
            if (isAlreadyCompleted(rootOrgId, formId, identifier)) {
                logger.info("Report already completed for identifier: {}. Skipping processing.", identifier);
                return;
            }

            // 3. Fetch questions from ElasticSearch and sort by order
            List<QuestionInfo> questions = fetchQuestions(formId);
            if (questions.isEmpty()) {
                String errorMsg = "No questions found for formId: " + formId;
                logger.error(errorMsg);
                updateCassandraStatus(rootOrgId, formId, identifier, Constants.FAILED_UPPERCASE,
                        errorMsg, 0, 0, 0, null, null, null, requestedBy);
                return;
            }

            logger.info("Fetched {} questions for formId: {}", questions.size(), formId);

            // 4. Generate CSV file
            File csvFile = generateCSVReport(formId, identifier, questions);

            // 5. Count total records processed
            int totalRecords = countLinesInFile(csvFile) - 1; // Subtract header row

            String bucketName = serverProperties.getCloudPublicContainerName();
            String objectKey = serverProperties.getPeerValidationCloudFolderName() + Constants.SEPARATOR_SLASH
                    + rootOrgId + Constants.SEPARATOR_SLASH
                    + formId;

            // 6. Upload CSV to storage
            SBApiResponse uploadResponse = storageService.uploadFile(csvFile,
                    objectKey,
                    bucketName);

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

            // 7. Update Cassandra with success status
            updateCassandraStatus(rootOrgId, formId, identifier, Constants.COMPLETED_STATUS,
                    null, totalRecords, totalRecords, 0, fileName, null, artifactUrl, requestedBy);

            logger.info("Report generation completed successfully for identifier: {}", identifier);

        } catch (Exception e) {
            logger.error("Error while generating report. rootOrgId: {}, formId: {}, identifier: {}",
                    rootOrgId, formId, identifier, e);

            String errorMsg = e.getMessage();
            if (errorMsg != null && errorMsg.length() > MAX_ERROR_MESSAGE_LENGTH) {
                errorMsg = errorMsg.substring(0, MAX_ERROR_MESSAGE_LENGTH);
            }

            if (rootOrgId != null && formId != null && identifier != null) {
                updateCassandraStatus(rootOrgId, formId, identifier, Constants.FAILED_UPPERCASE,
                        errorMsg, 0, 0, 0, null, null, null, requestedBy);
            }
        }
    }

    /**
     * Check if the report is already completed (idempotency check)
     */
    private boolean isAlreadyCompleted(String rootOrgId, String formId, String identifier) {
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
            propertyMap.put(Constants.FORM_ID_LOWER, formId);
            propertyMap.put(Constants.IDENTIFIER, identifier);

            List<String> fields = Arrays.asList(Constants.STATUS, Constants.ARTIFACT_URL);
            List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD, Constants.USER_SURVEY_REPORT, propertyMap, fields);

            if (records != null && !records.isEmpty()) {
                Map<String, Object> record = records.get(0);
                String status = (String) record.get(Constants.STATUS);
                String artifactUrl = (String) record.get(Constants.ARTIFACT_URL);

                return Constants.COMPLETED_STATUS.equalsIgnoreCase(status) && StringUtils.isNotBlank(artifactUrl);
            }
        } catch (Exception e) {
            logger.error("Error checking idempotency for identifier: {}", identifier, e);
        }
        return false;
    }

    /**
     * Fetch questions from ElasticSearch
     */
    private List<QuestionInfo> fetchQuestions(String formId) throws IOException {
        List<QuestionInfo> questions = new ArrayList<>();

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

                String questionId = hit.getId(); // Use document _id as questionId
                String name = (String) source.get(Constants.NAME);
                int order = source.get(Constants.ORDER) != null ?
                        ((Number) source.get(Constants.ORDER)).intValue() : 0;

                questions.add(new QuestionInfo(questionId, name, order));
            }
        }
        // Sort by order ascending
        questions.sort(Comparator.comparingInt(QuestionInfo::getOrder));

        logger.info("Fetched {} questions for formId: {}", questions.size(), formId);
        return questions;
    }

    /**
     * Generate CSV report with streaming
     */
    private File generateCSVReport(String formId, String identifier, List<QuestionInfo> questions) throws IOException {

        // Generate filename with date and time
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timestamp = dateFormat.format(new Date());
        String fileName = "user_survey_report_" + formId + "_" + timestamp + ".csv";
        File csvFile = new File(System.getProperty("java.io.tmpdir"), fileName);

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
                questionIdToColumnIndex.put(questions.get(i).getQuestionId(), i + 1); // +1 for full name column
            }

            // Fetch and write submissions (paginated)
            int totalSubmissions = 0;
            Object[] searchAfter = null;

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

                // Process each submission
                for (SearchHit hit : hits) {
                    Map<String, Object> submission = hit.getSourceAsMap();
                    List<String> rowValues = buildRowValues(submission, questions, questionIdToColumnIndex);
                    csvPrinter.printRecord(rowValues);
                    totalSubmissions++;
                }
                // Prepare for next page
                searchAfter = hits[hits.length - 1].getSortValues();
                logger.info("Processed {} submissions so far for formId: {}", totalSubmissions, formId);
            }
            logger.info("Total submissions written to CSV: {}", totalSubmissions);
        }
        return csvFile;
    }

    /**
     * Build CSV header row
     */
    private List<String> buildHeaders(List<QuestionInfo> questions) {
        List<String> headers = new ArrayList<>();

        // Add Full Name as first column
        headers.add(Constants.CSV_HEADER_FULL_NAME);

        // Add question headers (sorted by order)
        for (QuestionInfo question : questions) {
            headers.add(question.getName());
        }

        // Add peer review headers dynamically based on configuration
        int maxPeers = serverProperties.getPeerValidationReportMaxPeers();
        for (int i = 1; i <= maxPeers; i++) {
            headers.add(Constants.CSV_HEADER_PEER_PREFIX + i + " " + Constants.CSV_HEADER_PEER_DETAILS);
            headers.add(Constants.CSV_HEADER_PEER_PREFIX + i + " " + Constants.CSV_HEADER_PEER_RESPONSE);
        }

        return headers;
    }

    /**
     * Build CSV row values for a submission
     */
    private List<String> buildRowValues(Map<String, Object> submission,
                                        List<QuestionInfo> questions,
                                        Map<String, Integer> questionIdToColumnIndex) {

        int maxPeers = serverProperties.getPeerValidationReportMaxPeers();
        int peerColumns = maxPeers * 2; // Each peer has 2 columns (Details and Response)

        // Initialize row with empty values (1 user column + N question columns + peer columns)
        String[] rowArray = new String[1 + questions.size() + peerColumns];
        Arrays.fill(rowArray, "");

        // Fill Full Name (column 0)
        rowArray[0] = getStringValue(submission.get(Constants.USER_FULL_NAME));

        // Build questionId -> answer map from submission responses
        List<Map<String, Object>> responses = (List<Map<String, Object>>) submission.get(Constants.SUBMISSION_RESPONSES);
        if (responses != null) {
            for (Map<String, Object> response : responses) {
                String questionId = (String) response.get(Constants.SUBMISSION_QUESTION_ID);
                String answer = getStringValue(response.get(Constants.ANSWER));

                Integer columnIndex = questionIdToColumnIndex.get(questionId);
                if (columnIndex != null && columnIndex < rowArray.length) {
                    rowArray[columnIndex] = answer;
                }
            }
        }

        // Fill peer review columns (after questions, starting at index 1 + questions.size())
        List<Map<String, Object>> peerReviews = (List<Map<String, Object>>) submission.get(Constants.PEER_REVIEWS);
        if (peerReviews != null) {
            int peerStartIndex = 1 + questions.size();
            for (int i = 0; i < Math.min(maxPeers, peerReviews.size()); i++) {
                Map<String, Object> peer = peerReviews.get(i);
                if (peer != null) {
                    int peerDetailsIndex = peerStartIndex + (i * 2);
                    int peerResponseIndex = peerStartIndex + (i * 2) + 1;

                    // Fetch firstName from user_alias index instead of using peerId directly
                    String peerId = getStringValue(peer.get(Constants.PEER_ID));
                    String peerFirstName = getUserFirstName(peerId);

                    rowArray[peerDetailsIndex] = StringUtils.isNotBlank(peerFirstName) ? peerFirstName : peerId;
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

            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_SURVEY_REPORT, updateRecord, compositeKey);
            logger.info("Updated Cassandra status to {} for identifier: {}", status, identifier);
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
     * Fetch user's firstName from user_alias index
     */
    private String getUserFirstName(String userId) {
        if (StringUtils.isBlank(userId)) {
            return "";
        }

        String firstName = "";
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
                firstName = getStringValue(source.get(Constants.USER_FIRST_NAME));
            }
        } catch (Exception e) {
            logger.error("Error fetching user firstName for userId: {}", userId, e);
        }

        return firstName;
    }

    /**
     * Inner class to hold question information
     */
    private static class QuestionInfo {
        private final String questionId;
        private final String name;
        private final int order;

        public QuestionInfo(String questionId, String name, int order) {
            this.questionId = questionId;
            this.name = name;
            this.order = order;
        }

        public String getQuestionId() {
            return questionId;
        }

        public String getName() {
            return name;
        }

        public int getOrder() {
            return order;
        }
    }
}
