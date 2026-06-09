package org.sunbird.bpreports.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;
import org.sunbird.bpreports.postgres.entity.WfStatusEntity;
import org.sunbird.bpreports.postgres.repository.WfStatusEntityRepository;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.IndexerService;
import org.sunbird.common.util.ProjectUtil.ESIndexType;
import org.sunbird.storage.service.StorageService;

import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Component
public class BPReportConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BPReportConsumer.class);

    private final ObjectMapper mapper;
    private final WfStatusEntityRepository wfStatusEntityRepository;
    private final CassandraOperation cassandraOperation;
    private final IndexerService indexerService;
    private final CbExtServerProperties serverProperties;
    private final StorageService storageService;
    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final BPReportsServiceV2 bpReportsServiceV2;


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
            if (!errList.isEmpty()) {
                logger.error("Error in the Kafka Message Received for BP Report Generation: {}", errList);
                return;
            }
            String version = (String) request.getOrDefault(Constants.BP_REPORT_VERSION, Constants.BP_REPORT_VERSION_V1);
            switch (version) {
                case Constants.BP_REPORT_VERSION_V1:
                    generateBPReport(request);
                    break;
                case Constants.BP_REPORT_VERSION_V2:
                    generateBPReportV2(request);
                    break;
                default:
                    logger.error("BPReportConsumer: Unknown report version '{}', skipping event", version);
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

        int pendingUserCount = 0;
        int rejectedUserCount = 0;
        int approvedUserCount = 0;
        Map<String, Object> headerKeyMapping = new LinkedHashMap<>();
        String courseId = (String) request.get(Constants.COURSE_ID);
        String batchId = (String) request.get(Constants.BATCH_ID);
        String adminOrgId = (String) request.get(Constants.ORG_ID);
        String reportRequester = (String) request.get(Constants.REPORT_REQUESTER);

        try (Workbook workbook = new XSSFWorkbook()) {

            Map<String, Object> batchReadResp = getBatchDetails(courseId, batchId);
            if (ObjectUtils.isEmpty(batchReadResp)) {
                logger.info("No batch details found for batchId: {}", batchId);
                updateDataBase(adminOrgId, courseId, batchId, reportRequester, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
                return;
            }
            List<String> createdFor = (List<String>) batchReadResp.get(Constants.CREATED_FOR);
            String contentOrgid = createdFor.get(0);
            List<WfStatusEntity> wfStatusEntities = getAllWfStatusEntitiesByBatchId(batchId);
            if (CollectionUtils.isEmpty(wfStatusEntities)) {
                logger.info("No workflow status entities found for batchId: {}", batchId);
            }

            String surveyId = (String) request.get(Constants.SURVEY_ID);

            // Get BP survey form questions if survey ID is present
            List<Map<String, Object>> formQuestions = new ArrayList<>();
            if (StringUtils.isNotBlank(surveyId)) {
                formQuestions = getFormQuestionsList(surveyId);
                if (CollectionUtils.isEmpty(formQuestions)) {
                    logger.info("Survey form questions not found for surveyId: {}", surveyId);
                } else {
                    logger.info("Loaded {} survey form questions for surveyId: {}", formQuestions.size(), surveyId);
                }
            } else {
                logger.info("surveyId is null or empty. BP enrolment report will be generated using profile survey details only.");
            }

            //Get BP Profile survey data
            String batchAttributesStr = (String) batchReadResp.get(Constants.BATCH_ATTRIBUTES);
            if (StringUtils.isEmpty(batchAttributesStr)) {
                logger.error("No batch attributes found for batchId: {}", batchId);
                updateDataBase(adminOrgId, courseId, batchId, reportRequester, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
                return;
            }
            Map<String, Object> batchAttributes = mapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {
            });
            String profileSurveyId = (String) batchAttributes.get(Constants.PROFILE_SURVEY_ID);
            Map<String, Object> userBatchFormData = getUserBatchFormData(profileSurveyId);

            // Create header row and apply styles
            Sheet sheet = workbook.createSheet("Enrollment Report");
            if (Constants.MDO_ADMIN.equalsIgnoreCase(reportRequester) || Constants.MDO_LEADER.equalsIgnoreCase(reportRequester)) {
                createHeaderRowWithDefaultFields(workbook, sheet, formQuestions, headerKeyMapping);
            } else {
                mandatoryProfileFieldsKeyMappingForPC(batchAttributes, headerKeyMapping);
                createHeaderRow(workbook, sheet, formQuestions, headerKeyMapping);
            }
            int rowNum = 1;

            for (WfStatusEntity wfStatusEntity : wfStatusEntities) {
                String currentStatus = wfStatusEntity.getCurrentStatus();
                if (Constants.WITHDRAWN.equalsIgnoreCase(currentStatus)) {
                    continue;
                }
                String userId = wfStatusEntity.getUserId();
                if (StringUtils.isBlank(userId)) {
                    logger.warn("User ID is blank for WfStatusEntity: {}", wfStatusEntity);
                    continue;
                }

                try {
                    Map<String, Object> propertyMap = new HashMap<>();
                    propertyMap.put(Constants.ID, userId);
                    Map<String, Object> userDetails = cassandraOperation.getRecordsByProperties(
                            Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_USER, propertyMap, null, Constants.ID);
                    if (MapUtils.isEmpty(userDetails)) {
                        logger.warn("No user details found for userId: {}", userId);
                        continue;
                    }
                    Map<String, Object> userDetailsObj = (Map<String, Object>) userDetails.get(userId);
                    String userOrgId = (String) userDetailsObj.get(Constants.ROOT_ORG_ID);
                    if (!adminOrgId.equalsIgnoreCase(contentOrgid) && !adminOrgId.equalsIgnoreCase(userOrgId)) {
                        continue;
                    }
                    if (Constants.APPROVED_UPPER_CASE.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
                        approvedUserCount++;
                    } else if (Constants.REJECTED_UPPER_CASE.equalsIgnoreCase(wfStatusEntity.getCurrentStatus())) {
                        rejectedUserCount++;
                    } else {
                        pendingUserCount++;
                    }
                    Map<String, Object> userQuestionsAnswer = StringUtils.isNotEmpty(surveyId) ? getUserQuestionsAnswer(surveyId, userId, courseId) : new HashMap<>();
                    Map<String, Object> userInfo = getUserInfo(userDetailsObj);
                    String enrollmentStatus = getEnrollmentStatus(currentStatus);
                    processReport(userInfo, enrollmentStatus, MapUtils.isNotEmpty(userQuestionsAnswer) ? userQuestionsAnswer : new HashMap<>(), (Map<String, Object>) userBatchFormData.get(userId), sheet, headerKeyMapping, rowNum);

                } catch (Exception e) {
                    logger.error("Error processing report for userId: {}", userId, e);
                }
                rowNum++;
            }

            uploadBPReportAndUpdateDatabase(adminOrgId, courseId, batchId, reportRequester, workbook, pendingUserCount, approvedUserCount, rejectedUserCount);

        } catch (Exception e) {
            logger.error("Error processing report", e);
            updateDataBase(adminOrgId, courseId, batchId, reportRequester, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
        }
    }

    private Map<String, Object> getUserQuestionsAnswer(String surveyId, String userId, String contextId) {
        if (StringUtils.isNotEmpty(surveyId)) {
            List<Map<String, Object>> userFormData = getUserFormData(surveyId, userId, contextId);
            if (!CollectionUtils.isEmpty(userFormData)) {
                Map<String, Object> firstResponse = userFormData.get(0);
                if (!CollectionUtils.isEmpty((Collection<?>) firstResponse.get(Constants.RESPONSES))) {
                    List<Map<String, Object>> userFormQuestionsAnswerList =
                            (List<Map<String, Object>>) firstResponse.get(Constants.RESPONSES);

                    return userFormQuestionsAnswerList.stream()
                            .filter(Objects::nonNull)
                            .filter(qa -> qa.get(Constants.QUESTION) != null)
                            .collect(Collectors.toMap(
                                    qa -> (String) qa.get(Constants.QUESTION),
                                    qa -> qa.get(Constants.ANSWER),
                                    (existing, replacement) -> replacement,
                                    LinkedHashMap::new //preserves order
                            ));
                }
            }
        }
        return new LinkedHashMap<>();
    }

    private List<Map<String, Object>> getFormQuestionsList(String formId) {
        String url = String.format("%s%s?%s=%s",
                serverProperties.getFormServiceHost(),
                serverProperties.getGetFormByIdV2Path(),
                Constants.FORM_ID,
                formId);

        Map<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        headers.put(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);

        Map<String, Object> formReadResponse = (Map<String, Object>) outboundRequestHandlerService.fetchResultUsingGet(url, headers);
        if (MapUtils.isEmpty(formReadResponse) || !formReadResponse.containsKey(Constants.RESULT)) {
            return Collections.emptyList();
        }
        Map<String, Object> result = (Map<String, Object>) formReadResponse.get(Constants.RESULT);

        Map<String, Object> response = (Map<String, Object>) result.get(Constants.RESPONSE);
        if (MapUtils.isEmpty(response) || !response.containsKey(Constants.FIELDS)) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> fields = (List<Map<String, Object>>) response.get(Constants.FIELDS);
        if (CollectionUtils.isEmpty(fields)) {
            return Collections.emptyList();
        }

        return fields.stream()
                .filter(field -> Constants.QUESTION.equalsIgnoreCase(
                        (String) field.get(Constants.CONTEXT_TYPE)))
                .collect(Collectors.toList());
    }

    private Map<String, Object> getBatchDetails(String courseId, String batchId) {
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.COURSE_ID, courseId);
        propertyMap.put(Constants.BATCH_ID, batchId);

        List<String> fields = new ArrayList<>();
        fields.add(Constants.BATCH_ATTRIBUTES);
        fields.add(Constants.CREATED_FOR);

        List<Map<String, Object>> batchDetails = cassandraOperation.getRecordsByPropertiesWithoutFiltering(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.TABLE_COURSE_BATCH, propertyMap, fields);
        return batchDetails.get(0);
    }

    private void uploadBPReportAndUpdateDatabase(String orgId, String courseId, String batchId, String reportRequester, Workbook workbook, int pendingUserCount, int approvedUserCount, int rejectedUserCount) {
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

            updateDataBase(orgId, courseId, batchId, reportRequester, downloadUrl, fileName, Constants.COMPLETED_UPPER_CASE, pendingUserCount, approvedUserCount, rejectedUserCount, new Date());
        } catch (IOException e) {
            logger.error("Error while writing the Excel file", e);
            updateDataBase(orgId, courseId, batchId, reportRequester, null, null, Constants.FAILED_UPPERCASE, 0, 0, 0, new Date());
        }
    }

    private void updateDataBase(String orgId, String courseId, String batchId, String reportRequester, String downloadUrl, String fileName, String status, int pendingUserCount, int approvedUserCount, int rejectedUserCount, Date lastReportGeneratedOn) {
        Map<String, Object> compositeKey = new HashMap<>();
        compositeKey.put(Constants.ORG_ID, orgId);
        compositeKey.put(Constants.COURSE_ID, courseId);
        compositeKey.put(Constants.BATCH_ID, batchId);
        compositeKey.put(Constants.REPORT_REQUESTER, reportRequester);
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
                userInfo.put(Constants.PRIMARY_EMAIL, personalDetails.get(Constants.PRIMARY_EMAIL));
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
                userInfo.put(Constants.EXTERNAL_SYSTEM_ID, additionalProperties.get(Constants.EXTERNAL_SYSTEM_ID));
            }

            Map<String, Object> cadreDetails = (Map<String, Object>) profileDetails.get(Constants.CADRE_DETAILS);
            if (profileDetails.containsKey(Constants.CADRE_DETAILS) && MapUtils.isEmpty(cadreDetails)) {
                userInfo.put(Constants.CADRE_DETAILS, Constants.NO);
            } else if (MapUtils.isNotEmpty(cadreDetails)) {
                userInfo.put(Constants.CADRE_DETAILS, Constants.YES);
                userInfo.put(Constants.CIVIL_SERVICE_TYPE, cadreDetails.get(Constants.CIVIL_SERVICE_TYPE));
                userInfo.put(Constants.CIVIL_SERVICE_NAME, cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
                userInfo.put(Constants.CADRE_NAME, cadreDetails.get(Constants.CADRE_NAME));
                userInfo.put(Constants.CADRE_BATCH, cadreDetails.get(Constants.CADRE_BATCH));
                userInfo.put(Constants.CONTROLLING_AUTHORITY, cadreDetails.get(Constants.CONTROLLING_AUTHORITY));
            }

        }
        return userInfo;
    }

    private String getEnrollmentStatus(String currentStatus) {
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

    private List<Map<String, Object>> getUserFormData(String surveyId, String userId, String contextId) {
        List<Map<String, Object>> result = new ArrayList<>();
        try {
            // Build the search request
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

            // Build the base query
            BoolQueryBuilder finalQuery = QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery(Constants.FORM_ID, surveyId))
                    .must(QueryBuilders.termQuery(Constants.CONTEXT_ID_CAMEL, contextId))
                    .must(QueryBuilders.termQuery(Constants.SUBMITTED_BY, userId));

            // Apply query and sorting
            sourceBuilder.query(finalQuery);
            sourceBuilder.sort(Constants.SUBMITTED_DATE, SortOrder.DESC); // Sorting by timestamp in descending order

            // Execute the search request
            SearchResponse searchResponse = indexerService.getEsResult(
                    serverProperties.getUserFormDataIndexV2(),
                    serverProperties.getEsFormIndexType(),
                    sourceBuilder,
                    ESIndexType.IGOT_ES
            );

            // Process search results
            if (searchResponse != null && searchResponse.getHits().getHits().length > 0) {
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    if (sourceMap != null) {
                        result.add(sourceMap);
                    }
                }
            } else {
                logger.warn("No results found for surveyId: {} and userId: {}", surveyId, userId);
            }

        } catch (IOException e) {
            logger.error("Error while processing user form response for surveyId: {} and userId: {}", surveyId, userId, e);
        }

        return result;
    }

    private void processReport(Map<String, Object> userInfo, String enrollmentStatus, Map<String, Object> userQuestionsAnswer, Map<String, Object> userBatchFormData, Sheet sheet, Map<String, Object> headerKeyMapping, int rowNum) {
        try {
            // Create a new sheet or get existing one based on requirement
            Map<String, Object> reportInfo = prepareReportInfo(userInfo, enrollmentStatus, userQuestionsAnswer, userBatchFormData);

            // Add user data to the sheet
            fillDataRows(sheet, rowNum, headerKeyMapping, reportInfo);

        } catch (Exception e) {
            logger.error("Error while processing the report", e);
        }
    }

    private void createHeaderRow(Workbook workbook, Sheet sheet,
                                 List<Map<String, Object>> formQuestionsListMap, Map<String, Object> headerKeyMapping) throws IOException {

        Row headerRow = sheet.createRow(0);
        CellStyle headerStyle = createHeaderCellStyle(workbook);
        int currentColumnIndex = 0;

        // Populate header row with mandatory profile field display names
        for (Map.Entry<String, Object> entry : headerKeyMapping.entrySet()) {
            String displayName = (String) entry.getValue();
            if (displayName == null || displayName.isEmpty()) {
                throw new IllegalArgumentException("Profile field display name cannot be null or empty");
            }

            Cell cell = headerRow.createCell(currentColumnIndex++);
            cell.setCellValue(displayName.trim());
            cell.setCellStyle(headerStyle);
            sheet.autoSizeColumn(currentColumnIndex - 1);
        }

        // Add a column for Enrollment Status
        Cell cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.ENROLLMENT_STATUS_COLUMN);
        cell.setCellStyle(headerStyle);
        sheet.autoSizeColumn(currentColumnIndex - 1);
        headerKeyMapping.put(Constants.ENROLLMENT_STATUS, Constants.ENROLLMENT_STATUS_COLUMN);

        //Add profile survey group and designation
        cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.DESIGNATION_FILLED_DURING_ENROLLMENT);
        cell.setCellStyle(headerStyle);
        sheet.autoSizeColumn(currentColumnIndex - 1);
        headerKeyMapping.put(Constants.DESIGNATION_TITLE_CASE, Constants.DESIGNATION_FILLED_DURING_ENROLLMENT);

        cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.GROUP_FILLED_DURING_ENROLLMENT);
        cell.setCellStyle(headerStyle);
        sheet.autoSizeColumn(currentColumnIndex - 1);
        headerKeyMapping.put(Constants.GROUP_TITLE_CASE, Constants.GROUP_FILLED_DURING_ENROLLMENT);

        List<String> formQuestionsList = new ArrayList<>();

        // Populate header row with form questions that are not already mapped
        for (Map<String, Object> question : formQuestionsListMap) {
            String questionKey = (String) question.get(Constants.NAME);
            if (!headerKeyMapping.containsKey(questionKey)) {
                cell = headerRow.createCell(currentColumnIndex++);
                cell.setCellValue(questionKey.trim());
                cell.setCellStyle(headerStyle);
                sheet.autoSizeColumn(currentColumnIndex - 1);
                formQuestionsList.add(questionKey);
            }
        }

        headerKeyMapping.put(Constants.FORM_QUESTIONS, formQuestionsList);
    }

    private CellStyle createHeaderCellStyle(Workbook workbook) {
        // Create cell style for the header
        CellStyle headerStyle = workbook.createCellStyle();
        Font font = workbook.createFont();
        font.setBold(true);
        headerStyle.setFont(font);
        headerStyle.setAlignment(HorizontalAlignment.LEFT);
        headerStyle.setWrapText(true);
        return headerStyle;
    }

    private void fillDataRows(Sheet sheet, int rowNum, Map<String, Object> headerKeyMapping, Map<String, Object> reportInfo) {
        Row row = sheet.createRow(rowNum);
        int cellNum = 0;

        for (String columnKey : headerKeyMapping.keySet()) {
            if (columnKey.equalsIgnoreCase(Constants.FORM_QUESTIONS)) {
                Map<String, Object> formAllQuestionsAns = (Map<String, Object>) reportInfo.get(Constants.FORM_QUESTIONS_ANSWER);
                List<String> formAllRequiredQuestionskey = (List<String>) headerKeyMapping.get(columnKey);

                if (!CollectionUtils.isEmpty(formAllRequiredQuestionskey)) {
                    if (formAllQuestionsAns != null) {
                        for (String requiredQuestionKey : formAllRequiredQuestionskey) {
                            String answer = String.valueOf(formAllQuestionsAns.get(requiredQuestionKey));
                            row.createCell(cellNum++).setCellValue(StringUtils.isNotEmpty(answer) ? answer.trim() : "N/A");
                            sheet.autoSizeColumn(cellNum - 1);
                        }
                    } else {
                        row.createCell(cellNum++).setCellValue("No Questions/Ans Available");
                        sheet.autoSizeColumn(cellNum - 1);
                    }
                }
            } else {
                Object value = reportInfo.get(columnKey);
                row.createCell(cellNum++).setCellValue(!ObjectUtils.isEmpty(value) ? String.valueOf(value).trim() : "N/A");
            }
            sheet.autoSizeColumn(cellNum - 1);
        }
    }

    private Map<String, Object> prepareReportInfo(Map<String, Object> userInfo, String enrollmentStatus, Map<String, Object> userQuestionsAnswer, Map<String, Object> userBatchFormData) {

        Map<String, Object> reportInfo = new HashMap<>(userInfo);
        reportInfo.put(Constants.ENROLLMENT_STATUS, enrollmentStatus);

        // Extract and add survey questions and ans.
        if (MapUtils.isNotEmpty(userQuestionsAnswer)) {
            reportInfo.put(Constants.FORM_QUESTIONS_ANSWER, userQuestionsAnswer);
        }

        // Add batch for questions and ans.
        if (MapUtils.isNotEmpty(userBatchFormData)) {
            if (userBatchFormData.containsKey(Constants.GROUP_TITLE_CASE)) {
                reportInfo.put(Constants.GROUP_TITLE_CASE, userBatchFormData.get(Constants.GROUP_TITLE_CASE));
            }
            if (userBatchFormData.containsKey(Constants.DESIGNATION_TITLE_CASE)) {
                reportInfo.put(Constants.DESIGNATION_TITLE_CASE, userBatchFormData.get(Constants.DESIGNATION_TITLE_CASE));
            }

        }
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
        if (StringUtils.isEmpty((String) inputDataMap.get(Constants.REPORT_REQUESTER))) {
            errList.add("Report requester is missing");
        }
        if (!errList.isEmpty()) {
            str.append("Failed to Validate Course Batch Details. Error Details - [").append(errList.toString()).append("]");
        }
        return errList;
    }

    private void createHeaderRowWithDefaultFields(Workbook workbook, Sheet sheet,
                                                  List<Map<String, Object>> formQuestionsListMap, Map<String, Object> headerKeyMapping) throws IOException {

        String bpReportDefaultFieldsStr = serverProperties.getBpEnrolmentReportDefaultFields();
        Map<String, String> bpReportDefaultFieldsMap = mapper.readValue(bpReportDefaultFieldsStr, new TypeReference<LinkedHashMap<String, Object>>() {
        });
        headerKeyMapping.putAll(bpReportDefaultFieldsMap);
        Row headerRow = sheet.createRow(0);
        CellStyle headerStyle = createHeaderCellStyle(workbook);
        int currentColumnIndex = 0;

        // Populate header row with mandatory profile field display names
        for (Map.Entry<String, String> entry : bpReportDefaultFieldsMap.entrySet()) {
            String displayName = entry.getValue();
            if (displayName == null || displayName.isEmpty()) {
                throw new IllegalArgumentException("Profile field display name cannot be null or empty");
            }

            Cell cell = headerRow.createCell(currentColumnIndex++);
            cell.setCellValue(displayName.trim());
            cell.setCellStyle(headerStyle);
            sheet.autoSizeColumn(currentColumnIndex - 1);
        }

        // Add a column for Enrollment Status
        Cell cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.ENROLLMENT_STATUS_COLUMN);
        cell.setCellStyle(headerStyle);
        sheet.autoSizeColumn(currentColumnIndex - 1);
        headerKeyMapping.put(Constants.ENROLLMENT_STATUS, Constants.ENROLLMENT_STATUS_COLUMN);

        //Add profile survey group and designation
        cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.DESIGNATION_FILLED_DURING_ENROLLMENT);
        cell.setCellStyle(headerStyle);
        sheet.autoSizeColumn(currentColumnIndex - 1);
        headerKeyMapping.put(Constants.DESIGNATION_TITLE_CASE, Constants.DESIGNATION_FILLED_DURING_ENROLLMENT);

        cell = headerRow.createCell(currentColumnIndex++);
        cell.setCellValue(Constants.GROUP_FILLED_DURING_ENROLLMENT);
        cell.setCellStyle(headerStyle);
        sheet.autoSizeColumn(currentColumnIndex - 1);
        headerKeyMapping.put(Constants.GROUP_TITLE_CASE, Constants.GROUP_FILLED_DURING_ENROLLMENT);


        List<String> formQuestionsList = new ArrayList<>();

        // Populate header row with form questions that are not already mapped
        for (Map<String, Object> question : formQuestionsListMap) {
            String questionKey = (String) question.get(Constants.NAME);
            if (!headerKeyMapping.containsKey(questionKey)) {
                cell = headerRow.createCell(currentColumnIndex++);
                cell.setCellValue(questionKey.trim());
                cell.setCellStyle(headerStyle);
                sheet.autoSizeColumn(currentColumnIndex - 1);
                formQuestionsList.add(questionKey);
            }
        }

        headerKeyMapping.put(Constants.FORM_QUESTIONS, formQuestionsList);
    }

    private void mandatoryProfileFieldsKeyMappingForPC(Map<String, Object> batchAttributes, Map<String, Object> headerKeyMapping) throws IOException {

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
    }

    private Map<String, Object> getUserBatchFormData(String surveyId) {
        Map<String, Object> resultMap = new LinkedHashMap<>();
        final int BATCH_SIZE = 500;
        Object[] searchAfter = null;

        try {
            while (true) {
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();

                // Query
                BoolQueryBuilder finalQuery = QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery(Constants.FORM_ID, surveyId));

                sourceBuilder.query(finalQuery);
                sourceBuilder.sort(Constants.SUBMITTED_DATE, SortOrder.DESC);
                sourceBuilder.size(BATCH_SIZE);

                if (searchAfter != null) {
                    sourceBuilder.searchAfter(searchAfter);
                }

                SearchResponse searchResponse = indexerService.getEsResult(
                        serverProperties.getUserFormDataIndexV2(),
                        serverProperties.getEsFormIndexType(),
                        sourceBuilder,
                        ESIndexType.IGOT_ES
                );

                SearchHit[] hits = searchResponse.getHits().getHits();
                if (hits == null || hits.length == 0) {
                    break; // stop when no records found
                }

                for (SearchHit hit : hits) {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    if (sourceMap != null) {
                        List<Map<String, Object>> questionsAnswerList =
                                (List<Map<String, Object>>) sourceMap.get(Constants.RESPONSES);

                        Map<String, Object> questionsAnswerMap = questionsAnswerList.stream()
                                .filter(Objects::nonNull)
                                .filter(qa -> qa.get(Constants.QUESTION) != null)
                                .collect(Collectors.toMap(
                                        qa -> (String) qa.get(Constants.QUESTION),
                                        qa -> qa.get(Constants.ANSWER),
                                        (existing, replacement) -> replacement,
                                        LinkedHashMap::new
                                ));

                        resultMap.put((String) sourceMap.get(Constants.SUBMITTED_BY), questionsAnswerMap);
                    }
                }

                // Prepare next search_after value (use last hit’s sort values)
                searchAfter = hits[hits.length - 1].getSortValues();
            }

        } catch (IOException e) {
            logger.error("Error while processing user form response for surveyId: {}", surveyId, e);
        }

        logger.info("Total {} records found for batch surveyId: {}", resultMap.size(), surveyId);
        return resultMap;
    }

    /**
     * Delegates V2 report generation to {@link BPReportsServiceV2#processBPReportV2(Map)}.
     * Called when the Kafka message carries version="v2".
     *
     * @param request deserialized Kafka message payload containing courseId, batchId,
     *                orgId, reportRequester, and optional surveyId
     */
    private void generateBPReportV2(Map<String, Object> request) {
        bpReportsServiceV2.processBPReportV2(request);
    }
}