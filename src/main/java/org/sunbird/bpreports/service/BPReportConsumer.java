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
import org.springframework.beans.factory.annotation.Value;
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
import org.sunbird.storage.service.StorageService;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

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
    private void initiateBPReportGeneration(ConsumerRecord<String, String> data) {
        logger.info("AssessmentAsyncSubmitConsumer::processMessage.. started.");
        try {
            Map<String, Object> asyncRequest = mapper.readValue(data.value(), new TypeReference<Map<String, Object>>() {
            });
            generateBPReport(asyncRequest);
        } catch (Exception e) {
            String errMsg = String.format("Error while generating BP report", e.getMessage());
            logger.error(errMsg, e);
        }

    }

    private void generateBPReport(Map<String, Object> request) throws IOException {

        String courseId = (String) request.get(Constants.COURSE_ID);
        String batchId = (String) request.get(Constants.BATCH_ID);
        String orgId = (String) request.get(Constants.ORG_ID);
        String profileSurveyId = (String) request.get(Constants.PROFILE_SURVEY_ID);
        Workbook workbook = new XSSFWorkbook();
        Map<String, Object> headerKeyMapping = new LinkedHashMap<>();

        try {
            Map<String, Object> batchReadApiResp = getBatchDetails(courseId, batchId);
            if (ObjectUtils.isEmpty(batchReadApiResp)) {
                logger.info("No batch details found for batchId: {}", batchId);
                return;
            }
            if (StringUtils.isAnyBlank(batchId, profileSurveyId)) {
                throw new IllegalArgumentException("Batch ID and Profile Survey ID must not be empty.");
            }

            List<WfStatusEntity> wfStatusEntities = wfStatusEntityRepository.getByApplicationId(batchId);
            if (CollectionUtils.isEmpty(wfStatusEntities)) {
                logger.info("No workflow status entities found for batchId: {}", batchId);
                return;
            }

            Map<String, Object> surveyResponse = getSurveyResponse(profileSurveyId);
            Sheet sheet = workbook.createSheet("Enrolment Report");

            Iterator iterator = surveyResponse.entrySet().iterator();
            Map.Entry<String, Object> firstEntry = (Map.Entry<String, Object>) iterator.next();
            Map<String, Object> firstResponse = (Map<String, Object>) firstEntry.getValue();
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
                    Map<String, Object> propertyMap = new HashMap<>();
                    propertyMap.put(Constants.ID, userId);
                    Map<String, Object> userDetails = cassandraOperation.getRecordsByProperties(
                            Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_USER, propertyMap, null, Constants.ID);
                    if (userDetails == null || userDetails.isEmpty()) {
                        logger.warn("No user details found for userId: {}", userId);
                        continue;
                    }

                    Map<String, Object> userInfo = getUserInfo((Map<String, Object>) userDetails.get(userId));
                    String enrolmentStatus = getEnrolmentStatus(wfStatusEntity);
                    // Process or save the report with userInfo, enrolmentStatus, and surveyResponse.
                    processReport(userInfo, enrolmentStatus, (Map<String, Object>) surveyResponse.get(userId), sheet, headerKeyMapping, rowNum);

                } catch (Exception e) {
                    logger.error("Error processing report for userId: {}", userId, e);
                }
                rowNum++;
            }

            uploadBPReportAndUpdateDatabase(batchId, orgId, courseId, workbook);

        } catch (Exception e) {
            logger.error("Error processing report", e);
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

        List<Map<String, Object>> batchDetails = cassandraOperation.getRecordsByProperties(Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.TABLE_COURSE_BATCH, propertyMap, fields);
        return batchDetails.get(0);
    }

    private boolean uploadBPReportAndUpdateDatabase(String batchId, String orgId, String courseId, Workbook workbook) {
        String fileName;
        try {
            // Construct file name and path
            fileName = System.currentTimeMillis() + "-" + batchId + ".xlsx";
            String filePath = Constants.LOCAL_BASE_PATH + "bpreports" + "/" + orgId + "/" + courseId + "/";
            File directory = new File(filePath);

            // Check if directory exists, if not, create it
            if (!directory.exists()) {
                if (directory.mkdirs()) {
                    logger.info("Directory created: {}", filePath);
                } else {
                    logger.error("Failed to create directory: {}", filePath);
                    return true;
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
                    return true;
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
                return true;
            }

            SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getBpEnrolmentReportContainerName(), serverProperties.getCloudContainerName());
            String downloadUrl = (String) uploadResponse.getResult().get(Constants.URL);
            if (downloadUrl == null) {
                logger.error("Failed to upload file, download URL is null.");
                return true;
            }
            logger.info("File uploaded successfully. Download URL: {}", downloadUrl);


            Map<String, Object> compositeKey = new HashMap<>();
            compositeKey.put(Constants.ORG_ID, orgId);
            compositeKey.put(Constants.COURSE_ID, courseId);
            compositeKey.put(Constants.BATCH_ID, batchId);
            Map<String, Object> updateAttributes = new HashMap<>();
            updateAttributes.put(Constants.DOWNLOAD_LINK, downloadUrl);
            updateAttributes.put(Constants.FILE_NAME, fileName);
            cassandraOperation.updateRecord(Constants.SUNBIRD_KEY_SPACE_NAME, Constants.BP_ENROLMENT_REPORT_TABLE, compositeKey, updateAttributes);
        } catch (IOException e) {
            logger.error("Error while writing the Excel file", e);
        }
        return false;
    }

    private Map<String, Object> getUserInfo(Map<String, Object> userDetails) throws IOException {

        Map<String, Object> userInfo = new HashMap<>();
        userInfo.put(Constants.FIRSTNAME, userDetails.get(Constants.FIRSTNAME));

        String profileDetailsStr = (String) userDetails.get(Constants.PROFILE_DETAILS_LOWER);
        if (!StringUtils.isEmpty(profileDetailsStr)) {
            Map<String, Object> profileDetails = mapper.readValue(profileDetailsStr, new TypeReference<Map<String, Object>>() {
            });

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
                userInfo.put(Constants.GROUP, professionalDetailsObj.get(Constants.GROUP));
                userInfo.put(Constants.DESIGNATION, professionalDetailsObj.get(Constants.DESIGNATION));
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
                userInfo.put(Constants.CIVIL_SERVICE_TYPE, cadreDetails.get(Constants.CIVIL_SERVICE_TYPE));
                userInfo.put(Constants.CIVIL_SERVICE_NAME, cadreDetails.get(Constants.CIVIL_SERVICE_NAME));
                userInfo.put(Constants.CADRE_NAME, cadreDetails.get(Constants.CADRE_NAME));
            }

        }
        return userInfo;
    }

    private String getEnrolmentStatus(WfStatusEntity wfStatusEntity) {
        String currentStatus = wfStatusEntity.getCurrentStatus();
        if (currentStatus.equalsIgnoreCase("SEND_FOR_MDO_APPROVAL")) {
            return "Pending with MDO";
        } else if (currentStatus.equalsIgnoreCase("SEND_FOR_PC_APPROVAL")) {
            return "Pending with the Program Coordinator";
        } else if (currentStatus.equalsIgnoreCase("APPROVED")) {
            return "APPROVED";
        }
        return null;
    }

    private Map<String, Object> getSurveyResponse(String profileSurveyId) {
        Map<String, Object> resultMap = new HashMap<>();
        try {
            // Query construction
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.size(10000);  // Set the result size to 10000

            MatchQueryBuilder matchFormIdQuery = QueryBuilders.matchQuery(Constants.FORM_ID, profileSurveyId);
            BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(matchFormIdQuery);
            sourceBuilder.query(boolQuery);

            // Sorting by timestamp in ascending order
            sourceBuilder.sort("timestamp", SortOrder.ASC);

            // Execute the search request
            SearchResponse searchResponse = indexerService.getEsResult(serverProperties.getIgotEsUserFormIndex(), serverProperties.getEsFormIndexType(), sourceBuilder, false);

            if (searchResponse != null && searchResponse.getHits().getHits().length > 0) {
                // Process each record (hit)
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    if (sourceMap != null && sourceMap.containsKey(Constants.UPDATED_BY)) {
                        // Put the result in the map using the "updatedBy" field as the key
                        resultMap.put((String) sourceMap.get(Constants.UPDATED_BY), sourceMap);
                    }
                }
            } else {
                logger.warn("No results found for profileSurveyId: {}", profileSurveyId);
            }

        } catch (IOException e) {
            logger.error("Error while processing user form response for profileSurveyId: {}", profileSurveyId, e);
        }

        return resultMap;

    }

    private void processReport(Map<String, Object> userInfo, String enrolmentStatus, Map<String, Object> surveyResponse, Sheet sheet, Map<String, Object> headerKeyMapping, int rowNum) {
        logger.info("Report generation started for user: {}", userInfo);
        try {
            // Create a new sheet or get existing one based on requirement
            Map<String, Object> reportInfo = prepareReportInfo(userInfo, enrolmentStatus, surveyResponse);

            // Add user data to the sheet
            fillDataRows(sheet, rowNum, headerKeyMapping, reportInfo);

            // Auto-size columns for readability
            autoSizeColumns(sheet, headerKeyMapping.size());


        } catch (Exception e) {
            logger.error("Error while processing the report", e);
        }
    }

    private void createHeaderRow(Workbook workbook, Sheet sheet, Map<String, Object> batchDetails, Map<String, Object> formQuestionsMap, Map<String, Object> headerKeyMapping) throws IOException {
        Row headerRow = sheet.createRow(0);
        CellStyle headerStyle = createHeaderCellStyle(workbook);
        int currentColumnIndex = 0;
        String batchAttributesStr = (String) batchDetails.get("batch_attributes");
        Map<String, Object> batchAttributes = mapper.readValue(batchAttributesStr, new TypeReference<Map<String, Object>>() {
        });
        List<Map<String, Object>> bpEnrolMandatoryProfileFields = (List<Map<String, Object>>) batchAttributes.get(Constants.BATCH_ENROL_MANDATORY_PROFILE_FIELDS);
        for (Map<String, Object> bpEnrolMandatoryProfileField : bpEnrolMandatoryProfileFields) {
            Cell cell = headerRow.createCell(currentColumnIndex++);
            cell.setCellValue(bpEnrolMandatoryProfileField.get(Constants.DISPLAY_NAME).toString());
            cell.setCellStyle(headerStyle);
            String[] keyArr = ((String) bpEnrolMandatoryProfileField.get(Constants.FIELD)).split("\\.");
            headerKeyMapping.put(keyArr[keyArr.length - 1], bpEnrolMandatoryProfileField.get(Constants.DISPLAY_NAME));
        }

        List<String> formQuestions = new ArrayList<>();
        for (Map.Entry<String, Object> entry : formQuestionsMap.entrySet()) {
            if (!headerKeyMapping.containsKey(entry.getKey())) {
                Cell cell = headerRow.createCell(currentColumnIndex++);
                cell.setCellValue(entry.getKey());
                cell.setCellStyle(headerStyle);
                formQuestions.add(entry.getKey());

            }
        }
        headerKeyMapping.put("formQuestions", formQuestions);
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
                        row.createCell(cellNum++).setCellValue(formAllQuestionsAns.get(requiredQuestionKey).toString());
                    }
                } else {
                    row.createCell(cellNum++).setCellValue("No Questions Available");
                }
            } else {
                Object value = reportInfo.get(columnKey);
                if (value != null) {
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

    private Map<String, Object> prepareReportInfo(Map<String, Object> userInfo, String enrolmentStatus, Map<String, Object> surveyResponse) {

        Map<String, Object> reportInfo = new HashMap<>();

        reportInfo.putAll(userInfo);
        reportInfo.put(Constants.ENROLMENT_STATUS, enrolmentStatus);

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

    private Map<String, String> prepareColumnsKeyMapping(String columnsKeyMappingStr) throws IOException {
        return mapper.readValue(columnsKeyMappingStr, new TypeReference<Map<String, String>>() {
        });

    }
}
