package org.sunbird.bpreports.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.bpreports.postgres.entity.WfStatusEntity;
import org.sunbird.bpreports.postgres.repository.WfStatusEntityRepository;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.IndexerService;
import org.sunbird.storage.service.StorageService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Component
public class BPReportConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BPReportConsumer.class);

    @Autowired
    ObjectMapper mapper;


    @Autowired
    WfStatusEntityRepository wfStatusEntityRepository;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    IndexerService indexerService;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    StorageService storageService;


    @KafkaListener(topics = "${kafka.topic.bp.report}", groupId = "${kafka.topic.bp.report.group}")
    private void processBPReportGenerationMessage(ConsumerRecord<String, String> data) {
        logger.info("BPReportConsumer::processMessage.. started.");
        try {
            if (StringUtils.isNotBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    try {
                        logger.info("BP report generation initiated successfully for data: {}", data.value());
                        initiateBPReportGeneration(data.value());
                    } catch (Exception e) {
                        logger.error("Error while generating BP report for data: {}", data.value(), e);
                    }
                });
            } else {
                logger.error("Error in BPReportConsumer: Invalid or empty Kafka message received");
            }
        } catch (Exception e) {
            logger.error("Error while initiating BP report generation", e);
        }
    }

    public void initiateBPReportGeneration(String inputData) {
        logger.info("BPReportConsumer:: initiateBPReportGeneration: Started");
        long duration = 0;
        long startTime = System.currentTimeMillis();
        try {
            Map<String, Object> request = mapper.readValue(inputData, new TypeReference<Map<String, Object>>() {
            });
            List<String> errList = validateReceivedKafkaMessage(request);
            if (errList.isEmpty()) {
                generateBPReport(request);
            } else {
                logger.error(String.format("Error in the Kafka Message Received for BP Report Generation: %s", errList));
            }
        } catch (Exception e) {
            logger.error(String.format("Error in the scheduler to generate the BP report %s", e.getMessage()),
                    e);
        }
        duration = System.currentTimeMillis() - startTime;
        logger.info("BPReportConsumer:: initiateBPReportGeneration: Completed. Time taken: "
                + duration + " milli-seconds");
    }

    public void generateBPReport(Map<String, Object> request) throws IOException {

        Workbook workbook = new XSSFWorkbook();
        int pendingUserCount = 0;
        int rejectedUserCount = 0;
        int approvedUserCount = 0;
        Map<String, Object> headerKeyMapping = new LinkedHashMap<>();
        String courseId = (String) request.get(Constants.COURSE_ID);
        String batchId = (String) request.get(Constants.BATCH_ID);
        String orgId = (String) request.get(Constants.ORG_ID);
        String surveyId = (String) request.get(Constants.SURVEY_ID);
        try {

            Map<String, Object> batchReadApiResp = getBatchDetails(courseId, batchId);
            if (ObjectUtils.isEmpty(batchReadApiResp)) {
                logger.info("No batch details found for batchId: {}", batchId);
                updateDataBase(orgId, courseId, batchId, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
                return;
            }

            List<WfStatusEntity> wfStatusEntities = getAllWfStatusEntitiesByBatchId(batchId);
            if (CollectionUtils.isEmpty(wfStatusEntities)) {
                logger.info("No workflow status entities found for batchId: {}", batchId);
                updateDataBase(orgId, courseId, batchId, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
                return;
            }

            List<Map<String, Object>> surveyResponse = getSurveyResponse(surveyId, null);
            Sheet sheet = workbook.createSheet("Enrollment Report");

            Map<String, Object> firstResponse = surveyResponse.get(0);
            Map<String, Object> dataObject = (Map<String, Object>) firstResponse.get(Constants.DATA_OBJECT);

            // Create header row and apply styles
            createHeaderRow(workbook, sheet, batchReadApiResp, dataObject, headerKeyMapping);
            int rowNum = 1;

            for (WfStatusEntity wfStatusEntity : wfStatusEntities) {
                String userId = wfStatusEntity.getUserId();
                if (StringUtils.isBlank(userId)) {
                    logger.warn("User ID is blank for WfStatusEntity: {}", wfStatusEntity);
                    continue;
                }

                try {
                    if (Constants.APPROVED_UPPER_CASE.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
                        approvedUserCount++;
                    } else if (Constants.REJECTED_UPPER_CASE.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
                        rejectedUserCount++;
                    } else {
                        pendingUserCount++;
                    }
                    Map<String, Object> propertyMap = new HashMap<>();
                    propertyMap.put(Constants.ID, userId);
                    Map<String, Object> userDetails = cassandraOperation.getRecordsByProperties(
                            Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_USER, propertyMap, null, Constants.ID);
                    if (userDetails == null || userDetails.isEmpty()) {
                        logger.warn("No user details found for userId: {}", userId);
                        continue;
                    }
                    List<Map<String, Object>> userSurveyResponse = getSurveyResponse(surveyId, null);
                    Map<String, Object> userInfo = getUserInfo((Map<String, Object>) userDetails.get(userId));
                    String enrollmentStatus = getEnrollmentStatus(wfStatusEntity);
                    // Process or save the report with userInfo, enrollmentStatus, and surveyResponse.
                    processReport(userInfo, enrollmentStatus, userSurveyResponse.get(0), sheet, headerKeyMapping, rowNum);

                } catch (Exception e) {
                    logger.error("Error processing report for userId: {}", userId, e);
                }
                rowNum++;
            }

            uploadBPReportAndUpdateDatabase(batchId, orgId, courseId, workbook, pendingUserCount, approvedUserCount, rejectedUserCount);

        } catch (Exception e) {
            logger.error("Error processing report", e);
            updateDataBase(orgId, courseId, batchId, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
        } finally {
            // Closing workbook to prevent memory leaks
            try {
                workbook.close();
            } catch (IOException e) {
                logger.error("Error while closing the workbook", e);
            }
        }

    }

    private Map<String, Object> getBatchDetails(String courseId, String batchId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.COURSE_ID, courseId);
        propertyMap.put(Constants.BATCH_ID, batchId);

        List<String> fields = new ArrayList<>();
        fields.add("batch_attributes");

        List<Map<String, Object>> batchDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.TABLE_COURSE_BATCH, propertyMap, fields);
        return batchDetails.get(0);
    }

    private void uploadBPReportAndUpdateDatabase(String batchId, String orgId, String courseId, Workbook workbook, int pendingUserCount, int approvedUserCount, int rejectedUserCount) {
        String fileName;
        try {
            // Construct file name and path
            fileName = System.currentTimeMillis() + "_" + batchId + ".xlsx";
            String filePath = Constants.LOCAL_BASE_PATH + "bpreports" + "/" + orgId + "/" + courseId + "/";
            File directory = new File(filePath);

            // Check if directory exists, if not, create it
            if (!directory.exists()) {
                if (directory.mkdirs()) {
                    logger.info("Directory created: {}", filePath);
                } else {
                    logger.error("Failed to create directory: {}", filePath);
                    return;
                }
            } else {
                logger.info("Directory already exists: {}", filePath);
            }

            // Create file within the directory
            File file = new File(directory, fileName);
            if (!file.exists()) {
                boolean isFileCreated = file.createNewFile();
                if (isFileCreated) {
                    logger.info("File created: {}", file.getAbsolutePath());
                } else {
                    logger.error("Failed to create file: {}", fileName);
                    return;
                }
            } else {
                logger.info("File already exists, overwriting: {}", file.getAbsolutePath());
            }

            // Use try-with-resources to handle the file output stream
            try (OutputStream fileOut = new FileOutputStream(file, false)) {
                workbook.write(fileOut); // Write the workbook data to file
                logger.info("Excel file generated successfully: {}", fileName);
            } catch (IOException e) {
                logger.error("Error while writing the Excel file: {}", fileName, e);
                return;
            }
            String cloudBaseFolder = serverProperties.getBpEnrolmentReportContainerName();
            String cloudFilePath = cloudBaseFolder + "/" + orgId + "/" + courseId + "/" + batchId;
            SBApiResponse uploadResponse = storageService.uploadFile(file, cloudFilePath, serverProperties.getCloudContainerName());
            String downloadUrl = (String) uploadResponse.getResult().get(Constants.URL);
            if (downloadUrl == null) {
                logger.error("Failed to upload file, download URL is null.");
                return;
            }
            logger.info("File uploaded successfully. Download URL: {}", downloadUrl);

            updateDataBase(orgId, courseId, batchId, downloadUrl, fileName, Constants.COMPLETED_UPPER_CASE, pendingUserCount, approvedUserCount, rejectedUserCount, new Date());
        } catch (IOException e) {
            logger.error("Error while writing the Excel file", e);
            updateDataBase(orgId, courseId, batchId, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
        }
    }

    private void updateDataBase(String orgId, String courseId, String batchId, String downloadUrl, String fileName, String status, int pendingUserCount, int approvedUserCount, int rejectedUserCount, Date lastReportGeneratedOn) {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.ORG_ID, orgId);
        compositeKey.put(Constants.COURSE_ID, courseId);
        compositeKey.put(Constants.BATCH_ID, batchId);
        Map<String, Object> updateAttributes = new HashMap<>();
        if (StringUtils.isNotEmpty(downloadUrl)) {
            updateAttributes.put(Constants.DOWNLOAD_LINK, downloadUrl);
        }
        if (StringUtils.isNotEmpty(fileName)) {
            updateAttributes.put(Constants.FILE_NAME, fileName);
        }
        updateAttributes.put(Constants.STATUS, status);
        updateAttributes.put(Constants.PENDING_USER, pendingUserCount);
        updateAttributes.put(Constants.APPROVED_USER, approvedUserCount);
        updateAttributes.put(Constants.REJECTED_USER, rejectedUserCount);
        updateAttributes.put(Constants.LAST_REPORT_GENERATED_ON, lastReportGeneratedOn);
        cassandraOperation.updateRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, updateAttributes, compositeKey);
    }

    private Map<String, Object> getUserInfo(Map<String, Object> userDetails) throws IOException {

        Map<String, Object> userInfo = new HashMap<>();
        userInfo.put(Constants.FIRSTNAME, userDetails.get(Constants.FIRSTNAME));

        String profileDetailsStr = (String) userDetails.get(Constants.PROFILE_DETAILS_LOWER);
        if (!StringUtils.isEmpty(profileDetailsStr)) {
            Map<String, Object> profileDetails = mapper.readValue(profileDetailsStr, new TypeReference<Map<String, Object>>() {
            });

            String groupStatus = (String) profileDetails.get(Constants.PROFILE_GROUP_STATUS);
            if (StringUtils.isNotEmpty(groupStatus) && Constants.VERIFIED.equalsIgnoreCase(groupStatus)) {
                groupStatus = Constants.VERIFIED_TITLE_CASE;
            } else {
                groupStatus = Constants.NOT_VERIFIED_TITLE_CASE;
            }
            String designationStatus = (String) profileDetails.get(Constants.PROFILE_DESIGNATION_STATUS);
            if (StringUtils.isNotEmpty(groupStatus) && Constants.VERIFIED.equalsIgnoreCase(designationStatus)) {
                designationStatus = Constants.VERIFIED_TITLE_CASE;
            } else {
                designationStatus = Constants.NOT_VERIFIED_TITLE_CASE;
            }

            Map<String, Object> personalDetails = (Map<String, Object>) profileDetails.get(Constants.PERSONAL_DETAILS);
            if (MapUtils.isNotEmpty(personalDetails)) {
                userInfo.put(Constants.EMAIL, personalDetails.get(Constants.PRIMARY_EMAIL));
                userInfo.put(Constants.MOBILE, personalDetails.get(Constants.MOBILE));
                userInfo.put(Constants.GENDER, personalDetails.get(Constants.GENDER));
                userInfo.put(Constants.DOB, personalDetails.get(Constants.DOB));
                userInfo.put(Constants.DOMICILE_MEDIUM, personalDetails.get(Constants.DOMICILE_MEDIUM));
                userInfo.put(Constants.CATEGORY, personalDetails.get(Constants.CATEGORY));
            }

            List<Map<String, Object>> professionalDetails = (List<Map<String, Object>>) profileDetails.get(Constants.PROFESSIONAL_DETAILS);
            if (!CollectionUtils.isEmpty(professionalDetails)) {
                Map<String, Object> professionalDetailsObj = professionalDetails.get(0);
                String group = "";
                String designation = "";
                if (StringUtils.isNotEmpty((String) professionalDetailsObj.get(Constants.GROUP)) && StringUtils.isNotEmpty(groupStatus)) {
                    group = professionalDetailsObj.get(Constants.GROUP) + " " + "(" + groupStatus + ")";
                }
                if (StringUtils.isNotEmpty((String) professionalDetailsObj.get(Constants.DESIGNATION)) && StringUtils.isNotEmpty(designationStatus)) {
                    designation = professionalDetailsObj.get(Constants.DESIGNATION) + " " + "(" + designationStatus + ")";
                }
                userInfo.put(Constants.GROUP, group);
                userInfo.put(Constants.DESIGNATION, designation);
                userInfo.put(Constants.DOR, professionalDetailsObj.get(Constants.DOR));
            }

            Map<String, Object> employmentDetails = (Map<String, Object>) profileDetails.get(Constants.EMPLOYMENT_DETAILS);
            if (MapUtils.isNotEmpty(employmentDetails)) {
                userInfo.put(Constants.DEPARTMENTNAME, employmentDetails.get(Constants.DEPARTMENTNAME));
                userInfo.put(Constants.EMPLOYEE_CODE, employmentDetails.get(Constants.EMPLOYEE_CODE));
                userInfo.put(Constants.PINCODE, employmentDetails.get(Constants.PINCODE));
            }

            Map<String, Object> additionalProperties = (Map<String, Object>) profileDetails.get(Constants.ADDITIONAL_PROPERTIES);
            if (MapUtils.isNotEmpty(additionalProperties)) {
                userInfo.put(Constants.EXTERNAL_SYSTEM_ID, additionalProperties.get(Constants.ADDITIONAL_PROPERTIES));
            }

            Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(Constants.CADRE_DETAILS);
            if (MapUtils.isNotEmpty(cadreDetails)) {
                userInfo.put(Constants.CADRE_DETAILS, Constants.YES);
                userInfo.put(Constants.CIVIL_SERVICE_TYPE, cadreDetails.get(Constants.CIVIL_SERVICE_TYPE));
                userInfo.put(Constants.CIVIL_SERVICE_NAME, cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
                userInfo.put(Constants.CADRE_NAME, cadreDetails.get(Constants.CADRE_NAME));
            } else {
                userInfo.put(Constants.CADRE_DETAILS, Constants.NO);
            }

        }
        return userInfo;
    }

    private String getEnrollmentStatus(WfStatusEntity wfStatusEntity) {
        String currentStatus = wfStatusEntity.getCurrentStatus();
        if (Constants.SEND_FOR_MDO_APPROVAL.equalsIgnoreCase(currentStatus)) {
            return Constants.PENDING_WITH_MDO;
        } else if (Constants.SEND_FOR_PC_APPROVAL.equalsIgnoreCase(currentStatus)) {
            return Constants.PENDING_WITH_PC;
        } else if (Constants.APPROVED_UPPER_CASE.equalsIgnoreCase(currentStatus)) {
            return Constants.APPROVED_UPPER_CASE;
        } else if (Constants.REJECTED_UPPER_CASE.equalsIgnoreCase(currentStatus)) {
            return Constants.REJECTED_UPPER_CASE;
        }
        return null;
    }

    private List<Map<String, Object>> getSurveyResponse(String surveyId, String userId) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            // Query construction
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // If userId is null, fetch only one record
            if (userId == null) {
                sourceBuilder.size(1);  // Set the result size to 1
            }

            // Build the query for surveyId
            MatchQueryBuilder matchFormIdQuery = QueryBuilders.matchQuery(Constants.FORM_ID, surveyId);
            BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(matchFormIdQuery);

            // If userId is provided, add it to the query
            if (userId != null) {
                MatchQueryBuilder matchUserIdQuery = QueryBuilders.matchQuery(Constants.UPDATED_BY, userId);
                boolQuery.must(matchUserIdQuery);
            }

            sourceBuilder.query(boolQuery);

            // Sorting by timestamp in ascending order
            sourceBuilder.sort("timestamp", SortOrder.ASC);

            // Execute the search request
            SearchResponse searchResponse = indexerService.getEsResult(
                    serverProperties.getIgotEsUserFormIndex(),
                    serverProperties.getEsFormIndexType(),
                    sourceBuilder,
                    false
            );

            if (searchResponse != null && searchResponse.getHits().getHits().length > 0) {
                // Process each record (hit)
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    if (sourceMap != null) {
                        // Put the result in the map using the "updatedBy" field as the key
                        result.add(sourceMap);
                    }
                }
            } else {
                logger.warn("No results found for surveyId: {}", surveyId);
            }

        } catch (IOException e) {
            logger.error("Error while processing user form response for surveyId: {}", surveyId, e);
        }

        return result;
    }

    private void processReport(Map<String, Object> userInfo, String enrollmentStatus, Map<String, Object> surveyResponse, Sheet sheet, Map<String, Object> headerKeyMapping, int rowNum) {
        try {
            // Create a new sheet or get existing one based on requirement
            Map<String, Object> reportInfo = prepareReportInfo(userInfo, enrollmentStatus, surveyResponse);

            // Add user data to the sheet
            fillDataRows(sheet, rowNum, headerKeyMapping, reportInfo);

            // Auto-size columns for readability
            autoSizeColumns(sheet, headerKeyMapping.size());

        } catch (Exception e) {
            logger.error("Error while processing the report", e);
        }
    }

    private void createHeaderRow(Workbook workbook, Sheet sheet, Map<String, Object> batchDetails,
                                 Map<String, Object> formQuestionsMap, Map<String, Object> headerKeyMapping) throws IOException {

        Row headerRow = sheet.createRow(0);
        CellStyle headerStyle = createHeaderCellStyle(workbook);
        int currentColumnIndex = 0;

        String batchAttributesStr = (String) batchDetails.get(Constants.BATCH_ATTRIBUTES);
        if (batchAttributesStr == null) {
            throw new IllegalArgumentException("Batch attributes cannot be null");
        }

        Map<String, Object> batchAttributes = mapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {
        });

        List<Map<String, Object>> mandatoryProfileFields = (List<Map<String, Object>>) batchAttributes.get(Constants.BATCH_ENROL_MANDATORY_PROFILE_FIELDS);
        if (mandatoryProfileFields == null) {
            throw new IllegalArgumentException("Mandatory profile fields cannot be null");
        }

        // Populate header row with mandatory profile field display names
        for (Map<String, Object> profileField : mandatoryProfileFields) {
            String displayName = profileField.get(Constants.DISPLAY_NAME).toString();
            if (displayName == null || displayName.isEmpty()) {
                throw new IllegalArgumentException("Profile field display name cannot be null or empty");
            }

            Cell cell = headerRow.createCell(currentColumnIndex++);
            cell.setCellValue(displayName);
            cell.setCellStyle(headerStyle);

            // Extract and map the last part of the field key to display name
            String[] fieldKeyParts = ((String) profileField.get(Constants.FIELD)).split("\\.");
            String fieldKey = fieldKeyParts[fieldKeyParts.length - 1];

            // Map the field key to the display name
            if (fieldKey.equalsIgnoreCase(Constants.FIRSTNAME)) {
                headerKeyMapping.put(Constants.FIRSTNAME, displayName);
            } else {
                headerKeyMapping.put(fieldKey, displayName);
            }
        }

        // Add a column for Enrollment Status
        Cell cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.ENROLLMENT_STATUS_COLUMN);
        cell.setCellStyle(headerStyle);
        headerKeyMapping.put(Constants.ENROLLMENT_STATUS, Constants.ENROLLMENT_STATUS_COLUMN);

        List<String> formQuestionsList = new ArrayList<>();

        // Populate header row with form questions that are not already mapped
        for (Map.Entry<String, Object> entry : formQuestionsMap.entrySet()) {
            String questionKey = entry.getKey();
            if (!headerKeyMapping.containsKey(questionKey)) {
                cell = headerRow.createCell(currentColumnIndex++);
                cell.setCellValue(questionKey);
                cell.setCellStyle(headerStyle);
                formQuestionsList.add(questionKey);
            }
        }

        headerKeyMapping.put("formQuestions", formQuestionsList);
    }

    private CellStyle createHeaderCellStyle(Workbook workbook) {
        // Create cell style for the header
        CellStyle headerStyle = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        headerStyle.setFont(font);
        headerStyle.setAlignment(HorizontalAlignment.CENTER);  // Optional: center align
        return headerStyle;
    }

    private void fillDataRows(Sheet sheet, int rowNum, Map<String, Object> headerKeyMapping, Map<String, Object> reportInfo) {
        Row row = sheet.createRow(rowNum);
        int cellNum = 0;

        for (String columnKey : headerKeyMapping.keySet()) {
            if (columnKey.equalsIgnoreCase("formQuestions")) {
                Map<String, Object> formAllQuestionsAns = (Map<String, Object>) reportInfo.get(columnKey);
                List<String> formAllRequiredQuestionskey = (List<String>) headerKeyMapping.get(columnKey);

                if (formAllRequiredQuestionskey != null && formAllQuestionsAns != null) {
                    for (String requiredQuestionKey : formAllRequiredQuestionskey) {
                        if (StringUtils.isNotEmpty(formAllQuestionsAns.get(requiredQuestionKey).toString())) {
                            row.createCell(cellNum++).setCellValue(formAllQuestionsAns.get(requiredQuestionKey).toString());
                        } else {
                            row.createCell(cellNum++).setCellValue("N/A");
                        }
                    }
                } else {
                    row.createCell(cellNum++).setCellValue("No Questions Available");
                }
            } else {
                Object value = reportInfo.get(columnKey);
                if (!ObjectUtils.isEmpty(value)) {
                    row.createCell(cellNum++).setCellValue(value.toString());
                } else {
                    row.createCell(cellNum++).setCellValue("N/A");
                }
            }
        }
    }

    private void autoSizeColumns(Sheet sheet, int columnCount) {
        for (int i = 0; i < columnCount; i++) {
            sheet.autoSizeColumn(i);
        }
    }

    private Map<String, Object> prepareReportInfo(Map<String, Object> userInfo, String enrollmentStatus, Map<String, Object> surveyResponse) {

        Map<String, Object> reportInfo = new HashMap<>(userInfo);
        reportInfo.put(Constants.ENROLLMENT_STATUS, enrollmentStatus);

        // Extract and add survey questions
        Map<String, Object> formQuestions = new LinkedHashMap<>();
        if (surveyResponse != null && surveyResponse.containsKey(Constants.DATA_OBJECT)) {
            Map<String, Object> surveyData = (Map<String, Object>) surveyResponse.get(Constants.DATA_OBJECT);

            if (surveyData != null) {
                for (Map.Entry<String, Object> surveyDetails : surveyData.entrySet()) {
                    formQuestions.put(surveyDetails.getKey(), surveyDetails.getValue());
                }
            }
        }
        reportInfo.put("formQuestions", formQuestions);

        return reportInfo;
    }

    public List<WfStatusEntity> getAllWfStatusEntitiesByBatchId(String batchId) {
        List<WfStatusEntity> wfStatusEntities = new ArrayList<>();
        int pageNumber = 0;
        int pageSize = 100;  // Set the page size to 100
        Page<WfStatusEntity> page;

        // Fetch records in pages and keep collecting until all records are fetched
        do {
            PageRequest pageable = PageRequest.of(pageNumber, pageSize);
            page = wfStatusEntityRepository.findByApplicationId(batchId, pageable);
            wfStatusEntities.addAll(page.getContent());
            pageNumber++;  // Move to the next page
        } while (page.hasNext());  // Keep fetching while there are more pages

        return wfStatusEntities;
    }

    private List<String> validateReceivedKafkaMessage(Map<String, Object> inputDataMap) {
        StringBuilder str = new StringBuilder();
        List<String> errList = new ArrayList<>();
        if (StringUtils.isEmpty((String) inputDataMap.get(Constants.ORG_ID))) {
            errList.add("OrgId is missing");
        }
        if (StringUtils.isEmpty((String) inputDataMap.get(Constants.COURSE_ID))) {
            errList.add("Course ID is missing");
        }
        if (StringUtils.isEmpty((String) inputDataMap.get(Constants.BATCH_ID))) {
            errList.add("Batch Id ID is missing");
        }
        if (StringUtils.isEmpty((String) inputDataMap.get(Constants.SURVEY_ID))) {
            errList.add("Survey ID is missing");
        }
        if (!errList.isEmpty()) {
            str.append("Failed to Validate Course Batch Details. Error Details - [").append(errList.toString()).append("]");
        }
        return errList;
    }
}
