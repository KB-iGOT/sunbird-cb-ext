package org.sunbird.org.consumer;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.storage.service.StorageService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Component
public class OrgHierarchyBulkUploadConsumer {

    @Autowired
    StorageService storageService;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    CassandraOperation cassandraOperation;

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandler;

    @Autowired
    RedisCacheMgr redisCacheMgr;

    private final Logger logger = LoggerFactory.getLogger(OrgHierarchyBulkUploadConsumer.class);


    @KafkaListener(topics = "${kafka.topics.org.hierarchy.bulk.upload.event}", groupId = "${kafka.topics.org.hierarchy.bulk.upload.event.group}")
    public void processOrgHierarchyBulkUploadMessage(ConsumerRecord<String, String> data) {
        logger.info(
                "processOrgHierarchyBulkUploadMessage::processMessage: Received event to initiate Process OrgHierarchy BulkUpload Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                CompletableFuture.runAsync(() -> {
                    initiateOrgHierarchyBulkUploadProcess(data.value());
                });
            } else {
                logger.error("Error in Process OrgHierarchy BulkUpload Mapping Bulk Upload Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Process OrgHierarchy BulkUpload Message Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }

    public void initiateOrgHierarchyBulkUploadProcess(String value) {
        logger.info("initiateUserBulkUploadProcess: Started");
        long duration = 0;
        long startTime = System.currentTimeMillis();
        try {
            HashMap<String, String> inputDataMap = objectMapper.readValue(value,
                    new TypeReference<Object>() {
                    });
            List<String> errList = validateReceivedKafkaMessage(inputDataMap);
            if (errList.isEmpty()) {
                updateOrgHierarchyMappingBulkUploadStatus(inputDataMap.get(Constants.ROOT_ORG_ID),
                        inputDataMap.get(Constants.IDENTIFIER), Constants.STATUS_IN_PROGRESS_UPPERCASE, 0, 0, 0);
                storageService.downloadFile(inputDataMap.get(Constants.FILE_NAME), serverProperties.getOrgHierarchyBulkUploadContainerName());
                processBulkHierarchyUpload(inputDataMap);
            } else {
                logger.error(String.format("Error in the Kafka Message Received : %s", errList));
            }
        } catch (Exception e) {
            logger.error(String.format("Error in the scheduler to upload bulk users %s", e.getMessage()),
                    e);
        }
        duration = System.currentTimeMillis() - startTime;
        logger.info("initiateUserBulkUploadProcess: Completed. Time taken: "
                + duration + " milli-seconds");
    }

    public void updateOrgHierarchyMappingBulkUploadStatus(String rootOrgId, String identifier, String status, int totalRecordsCount,
                                                                      int successfulRecordsCount, int failedRecordsCount) {
        try {
            Map<String, Object> compositeKeys = new HashMap<>();
            compositeKeys.put(Constants.ROOT_ORG_ID_LOWER, rootOrgId);
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
            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.ORG_HIERARCHY_MAPPING_BULK_UPLOAD,
                    fieldsToBeUpdated, compositeKeys);
        } catch (Exception e) {
            logger.error(String.format("Error in Updating  Bulk Upload Status in Cassandra %s", e.getMessage()), e);
        }
    }


    private List<String> validateReceivedKafkaMessage(HashMap<String, String> inputDataMap) {
        StringBuilder str = new StringBuilder();
        List<String> errList = new ArrayList<>();
        if (StringUtils.isEmpty(inputDataMap.get(Constants.ROOT_ORG_ID))) {
            errList.add("RootOrgId is not present");
        }
        if (org.apache.commons.lang.StringUtils.isEmpty(inputDataMap.get(Constants.IDENTIFIER))) {
            errList.add("Identifier is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.FILE_NAME))) {
            errList.add("Filename is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.X_AUTH_TOKEN))) {
            errList.add("User Token is not present");
        }
        if (StringUtils.isEmpty(inputDataMap.get(Constants.FRAMEWORK_ID))) {
            errList.add("Framework ID is not present");
        }
        if (!errList.isEmpty()) {
            str.append("Failed to Validate User Details. Error Details - [").append(errList.toString()).append("]");
        }
        return errList;
    }

    private Map<String, Object> buildCreateTermRequest(
            String frameworkId,
            String name,
            String category,
            String parentTermId,
            String parentOrgName,
            String generateCode,
            String createdBy
    ) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.CODE, generateCode);
        termReq.put(Constants.CATEGORY, category);
        termReq.put(Constants.NAME, extractName(name));
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(Constants.ORG_ID, extractIdentifier(name));
        additionalProperties.put(Constants.PARENT_ORG_NAME, parentOrgName);
        additionalProperties.put(Constants.CREATED_BY, createdBy);
        termReq.put(Constants.ADDITIONAL_PROPERTIES, additionalProperties);
        requestBody.put(Constants.TERM, termReq);
        Map<String, Object> createReq = new HashMap<>();
        createReq.put(Constants.REQUEST, requestBody);
        return createReq;
    }

    private String uploadTheUpdatedFile(File file, XSSFWorkbook wb) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(file);
        wb.write(fileOut);
        fileOut.close();
        SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getOrgHierarchyBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.info(String.format("Failed to upload file. Error: %s", uploadResponse.getParams().getErrmsg()));
            return Constants.FAILED_UPPERCASE;
        }
        return Constants.SUCCESSFUL_UPPERCASE;
    }

    private static class TermPosition {
        String category;
        int levelIndex;
        Map<String, Object> term;
        TermPosition(String category, int levelIndex, Map<String, Object> term) {
            this.category = category;
            this.levelIndex = levelIndex;
            this.term = term;
        }
    }

    private void processBulkHierarchyUpload(HashMap<String, String> inputDataMap) throws Exception {
        File file = new File(Constants.LOCAL_BASE_PATH + inputDataMap.get(Constants.FILE_NAME));
        FileInputStream fis = new FileInputStream(file);
        XSSFWorkbook wb = new XSSFWorkbook(fis);
        XSSFSheet sheet = wb.getSheetAt(0);
        Map<String, Set<String>> parentToChildrenMap = new HashMap<>();

        int totalRecordsCount = 0;
        int noOfSuccessfulRecords = 0;
        int failedRecordsCount = 0;
        int totalNumberOfRecordInSheet = 0;

        Row headerRow = sheet.getRow(0);
        List<String> headers = new ArrayList<>();
        for (Cell cell : headerRow) {
            headers.add(cell.getStringCellValue().trim());
        }
        int lastHeaderCellNum = headerRow.getLastCellNum();
        headerRow.createCell(lastHeaderCellNum).setCellValue("Status");
        headerRow.createCell(lastHeaderCellNum + 1).setCellValue("Error");

        String currentFramework = inputDataMap.get(Constants.FRAMEWORK_ID);
        String orgId = currentFramework.split("_")[0];
        retireFramework(currentFramework, inputDataMap.get(Constants.CREATED_BY), orgId);

        String frameworkId = processFrameworkCreate(serverProperties.getOrgHierarchyMasterFramework(), orgId);
        logger.info("Framework created with ID: " + frameworkId);
        if (StringUtils.isBlank(frameworkId)) {
            logger.error("Failed to create or retrieve framework ID for org hierarchy bulk upload.");
            return;
        }
        String orgUpdateUrl = serverProperties.getSbUrl() + serverProperties.getUpdateOrgPath();
        Map<String, Object> orgResponse = outboundRequestHandler.fetchResultUsingPatch(orgUpdateUrl,createOrgHierarchyRequestMap(orgId, Constants.ORG_HIERARCHY_FRAMEWORK_ID_KEY, Constants.ORG_HIERARCHY_FRAMEWORK_STATUS_KEY, frameworkId, Constants.COMPLETED), ProjectUtil.getDefaultHeadrs(inputDataMap.get(Constants.X_AUTH_TOKEN)));
        if (org.apache.commons.collections.MapUtils.isNotEmpty(orgResponse) && Constants.OK.equalsIgnoreCase(
                (String) orgResponse.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) orgResponse.get(
                    Constants.RESULT);
            String orgResult = (String) result.getOrDefault(Constants.RESPONSE, "");
            logger.info("Organization updated successfully. orgId: {}, result: {}", orgId, orgResult);
        }
        String createdBy = inputDataMap.get(Constants.CREATED_BY);

        Iterator<Row> rowIterator = sheet.iterator();
        if (rowIterator.hasNext()) rowIterator.next();

        if (!rowIterator.hasNext()) {
            logger.error("No data rows found in the uploaded file.");
            updateOrgHierarchyMappingBulkUploadStatus(
                    orgId, inputDataMap.get(Constants.IDENTIFIER), Constants.FAILED_UPPERCASE,
                    0, 0, 0
            );
            int lastHeaderCell = headerRow.getLastCellNum();
            Row errorRow = sheet.createRow(1);
            errorRow.createCell(lastHeaderCell).setCellValue(Constants.FAILED_UPPERCASE);
            errorRow.createCell(lastHeaderCell + 1).setCellValue("File is empty");
            wb.close();
            fis.close();
            return;
        }

        List<Map<String, Object>> frameworkData = getMasterCompetencyFrameworkData(frameworkId);
        int levelCount = 10;

        Map<String, TermPosition> termPositionMap = new HashMap<>();

        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            List<String> levels = new ArrayList<>();
            List<String> categories = new ArrayList<>();
            for (int i = 0; i < levelCount; i++) {
                Cell cell = row.getCell(i, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                String value = cell.getCellType() == CellType.STRING ? cell.getStringCellValue().trim() : "";
                levels.add(value);
                categories.add(headers.get(i));
            }

            Cell statusCell = row.getCell(levelCount, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
            Cell errorCell = row.getCell(levelCount + 1, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);

            List<String> errors = new ArrayList<>();
            try {
                processHierarchyRow(levels, categories, frameworkId, frameworkData, errors, termPositionMap, createdBy);
                if (errors.isEmpty()) {
                    statusCell.setCellValue(Constants.SUCCESS_UPPERCASE);
                    errorCell.setCellValue("");
                    noOfSuccessfulRecords++;
                } else {
                    statusCell.setCellValue(Constants.FAILED_UPPERCASE);
                    errorCell.setCellValue(String.join(", ", errors));
                    failedRecordsCount++;
                }
            } catch (Exception e) {
                statusCell.setCellValue(Constants.FAILED_UPPERCASE);
                errorCell.setCellValue(e.getMessage());
                failedRecordsCount++;
            }
            totalRecordsCount++;
            totalNumberOfRecordInSheet++;
        }

        boolean publishSuccess = publishFramework(frameworkId, inputDataMap.get(Constants.X_AUTH_TOKEN), inputDataMap.get(Constants.ROOT_ORG_ID), wb, file, levelCount);
        String status = publishSuccess ? Constants.SUCCESSFUL_UPPERCASE : Constants.FAILED_UPPERCASE;

        updateOrgHierarchyMappingBulkUploadStatus(
                orgId, inputDataMap.get(Constants.IDENTIFIER), status,
                totalNumberOfRecordInSheet, noOfSuccessfulRecords, failedRecordsCount
        );

        uploadTheUpdatedFile(file, wb);

        wb.close();
        fis.close();
    }


    private void processHierarchyRow(List<String> levels, List<String> categories, String frameworkId,
                                     List<Map<String, Object>> frameworkData, List<String> errors,
                                     Map<String, TermPosition> termPositionMap, String createdBy) throws Exception {
        Map<String, Object> parentTerm = null;
        for (int i = 0; i < levels.size(); i++) {
            String levelName = levels.get(i);
            String category = categories.get(i);
            if (StringUtils.isBlank(levelName)) break;

            String identifier = extractIdentifier(levelName);
            String termKey = StringUtils.isNotBlank(identifier) ? identifier : levelName;

            if (termPositionMap.containsKey(termKey)) {
                TermPosition pos = termPositionMap.get(termKey);
                if (!pos.category.equals(category) || pos.levelIndex != i) {
                    errors.add("Term '" + levelName + "' appears in multiple levels/categories: " +
                            pos.category + " (L" + (pos.levelIndex + 1) + ") and " + category + " (L" + (i + 1) + ")");
                    return;
                }
                parentTerm = pos.term;
                continue;
            }

            Map<String, Object> currentTerm = findTermInFramework(frameworkData, category, levelName);
            if (MapUtils.isEmpty(currentTerm)) {
                String parentOrgName = (i > 0 && parentTerm != null) ? (String) parentTerm.get(Constants.NAME) : null;
                String generateCode = UUIDs.timeBased().toString();
                Map<String, Object> createReq = buildCreateTermRequest(frameworkId, levelName, category,
                        i > 0 ? levels.get(i-1) : null, parentOrgName, generateCode, createdBy);
                Map<String, Object> createResp = createFrameworkTerm(frameworkId, createReq, category);
                if (MapUtils.isNotEmpty(createResp) && createResp.containsKey(Constants.NODE_ID)) {
                    currentTerm = new HashMap<>();
                    currentTerm.put(Constants.IDENTIFIER, ((List<String>) createResp.get(Constants.NODE_ID)).get(0));
                    currentTerm.put(Constants.CODE, generateCode);
                    currentTerm.put(Constants.NAME, extractName(levelName));
                    Map<String, Object> additionalProps = new HashMap<>();
                    additionalProps.put(Constants.IDENTIFIER, identifier);
                    currentTerm.put(Constants.ADDITIONAL_PROPERTIES, additionalProps);
                    updateFrameworkDataWithNewTerm(frameworkData, category, currentTerm);
                } else {
                    errors.add("Failed to create term for " + levelName);
                    return;
                }
            }
            termPositionMap.put(termKey, new TermPosition(category, i, currentTerm));

            // Update parent-child associations
            if (i > 0 && parentTerm != null) {
                String parentCode = (String) parentTerm.get(Constants.CODE);
                String parentCategory = categories.get(i-1);
                Map<String, Object> parentTermInFramework = findTermInFramework(frameworkData, parentCategory, (String) parentTerm.get(Constants.NAME));
                List<Map<String, Object>> associations = parentTermInFramework != null &&
                        parentTermInFramework.get(Constants.ASSOCIATIONS) != null
                        ? new ArrayList<>((List<Map<String, Object>>) parentTermInFramework.get(Constants.ASSOCIATIONS))
                        : new ArrayList<>();
                boolean exists = false;
                for (Map<String, Object> assoc : associations) {
                    if (currentTerm.get(Constants.IDENTIFIER).equals(assoc.get(Constants.IDENTIFIER))) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    Map<String, Object> assoc = new HashMap<>();
                    assoc.put(Constants.IDENTIFIER, currentTerm.get(Constants.IDENTIFIER));
                    associations.add(assoc);
                    Map<String, Object> updateReq = updateRequestObject(associations);
                    Map<String, Object> updateResp = updateFrameworkTerm(frameworkId, updateReq, parentCategory, parentCode);
                    if (MapUtils.isEmpty(updateResp)) {
                        errors.add("Failed to associate " + levelName + " with parent " + parentTerm.get(Constants.NAME));
                        return;
                    }
                    updateAssociationsInFrameworkData(frameworkData, parentCategory, parentCode, associations);
                }
            }
            parentTerm = currentTerm;
        }
    }

    private void updateFrameworkDataWithNewTerm(List<Map<String, Object>> frameworkData, String category, Map<String, Object> newTerm) {
        for (Map<String, Object> categoryObj : frameworkData) {
            if (category.equalsIgnoreCase((String) categoryObj.get(Constants.NAME))) {
                List<Map<String, Object>> terms = (List<Map<String, Object>>) categoryObj.get(Constants.TERMS);
                if (terms == null) {
                    terms = new ArrayList<>();
                    categoryObj.put(Constants.TERMS, terms);
                }
                terms.add(newTerm);
                break;
            }
        }
    }

    private void updateAssociationsInFrameworkData(List<Map<String, Object>> frameworkData, String category, String parentCode, List<Map<String, Object>> associations) {
        for (Map<String, Object> categoryObj : frameworkData) {
            logger.info("Updating associations for category: " + category);
            logger.info("Parent code: " + parentCode);
            logger.info("Associations: " + associations);
            if (category.equalsIgnoreCase((String) categoryObj.get(Constants.NAME))) {
                List<Map<String, Object>> terms = (List<Map<String, Object>>) categoryObj.get(Constants.TERMS);
                if (terms != null) {
                    for (Map<String, Object> term : terms) {
                        if (parentCode.equals(term.get(Constants.CODE))) {
                            logger.info("Found term with code: " + parentCode);
                            term.put(Constants.ASSOCIATIONS, associations);
                            return;
                        }
                    }
                }
            }
        }
    }

    private void saveWorkbook(XSSFWorkbook wb, File file) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(file);
        wb.write(fileOut);
        fileOut.close();
    }

    private Map<String, Object> findTermInFramework(List<Map<String, Object>> frameworkData, String category, String name) {
        String identifier = extractIdentifier(name);
        logger.info("Extracted identifier: " + identifier);
        if (StringUtils.isNotBlank(identifier)) {
            Map<String, Object> termById = findTermInFrameworkByIdentifier(frameworkData, category, identifier);
            if (termById != null) {
                logger.info("Found term by identifier: " + identifier);
                return termById;
            }
        }

        Map<String, Object> categoryObj = frameworkData.stream()
                .filter(n -> category.equalsIgnoreCase((String) n.get(Constants.NAME)))
                .findFirst().orElse(null);
        if (MapUtils.isNotEmpty(categoryObj)) {
            List<Map<String, Object>> terms = (List<Map<String, Object>>) categoryObj.get(Constants.TERMS);
            if (CollectionUtils.isNotEmpty(terms)) {
                for (Map<String, Object> t : terms) {
                    String termName = (String) t.get(Constants.NAME);
                    if (extractName(name).equalsIgnoreCase(termName)) {
                        logger.info("Found term by name: " + termName);
                        return t;
                    }
                }
            }
        }
        logger.info("Term not found in framework data for category: " + category + ", name: " + name);
        return null;
    }

    private Map<String, Object> updateRequestObject(List<Map<String, Object>> associations) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.ASSOCIATIONS, associations);
        requestBody.put(Constants.TERM, termReq);
        Map<String, Object> createReq = new HashMap<>();
        createReq.put(Constants.REQUEST, requestBody);
        return createReq;
    }

    private Map<String, Object> updateFrameworkTerm(String frameworkId, Map<String, Object> createReq, String category, String code) throws Exception {
        StringBuilder strUrl = new StringBuilder(serverProperties.getKmBaseHost());
        strUrl.append(serverProperties.getKmFrameworkTermUpdatePath() + "/" + code).append("?framework=")
                .append(frameworkId).append("&category=")
                .append(category);
        logger.info("Updating framework Term with URL: " + strUrl.toString());
        logger.info("Request body for updating framework Term: " + objectMapper.writeValueAsString(createReq));
        Map<String, Object> termResponse = outboundRequestHandler.fetchResultUsingPatch(
                strUrl.toString(), createReq, null);
        if (MapUtils.isNotEmpty(termResponse)
                && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
            logger.info("Updated framework Term successfully");
            return (Map<String, Object>) termResponse.get(Constants.RESULT);
        } else {
            logger.error("Failed to update the framework object: " + objectMapper.writeValueAsString(createReq));
        }
        return null;
    }

    private Map<String, Object> createFrameworkTerm(String frameworkId, Map<String, Object> createReq, String category) throws Exception {
        StringBuilder strUrl = new StringBuilder(serverProperties.getKmBaseHost());
        strUrl.append(serverProperties.getKmFrameworkTermCreatePath()).append("?framework=")
                .append(frameworkId).append("&category=")
                .append(category);
        Map<String, Object> termResponse = outboundRequestHandler.fetchResultUsingPost(
                strUrl.toString(), createReq, null);
        if (MapUtils.isNotEmpty(termResponse)
                && Constants.OK.equalsIgnoreCase((String) termResponse.get(Constants.RESPONSE_CODE))) {
            logger.info("Created framework Term successfully");
            return (Map<String, Object>) termResponse.get(Constants.RESULT);
        } else {
            logger.error("Failed to create the framework term: " + objectMapper.writeValueAsString(createReq));
        }
        return null;
    }

    private List<Map<String, Object>> getMasterCompetencyFrameworkData(String frameworkId) throws Exception {
            List<Map<String, Object>> masterData = populateDataFromFrameworkTerm(frameworkId);
            return masterData;
    }

    private List<Map<String, Object>> populateDataFromFrameworkTerm(String frameworkName) throws Exception {
        String url = serverProperties.getKmBaseHost() + serverProperties.getKmFrameWorkPath() + "/" + frameworkName;
        Map<String, Object> response = (Map<String, Object>) outboundRequestHandler.fetchUsingGetWithHeaders(url.toString(), null);
        if (MapUtils.isNotEmpty(response) && Constants.OK.equalsIgnoreCase((String) response.get(Constants.RESPONSE_CODE))) {
            Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
            if (MapUtils.isNotEmpty(result) && result.containsKey(Constants.FRAMEWORK)) {
                Map<String, Object> framework = (Map<String, Object>) result.get(Constants.FRAMEWORK);
                if (framework.containsKey(Constants.CATEGORIES)) {
                    return (List<Map<String, Object>>) framework.get(Constants.CATEGORIES);
                }
            }
        }
        logger.error("Failed to fetch framework data for: " + frameworkName);
        return Collections.emptyList();
    }

    private String extractIdentifier(String cellValue) {
        if (cellValue == null) return null;
        java.util.regex.Matcher matcher = java.util.regex.Pattern.compile("\\(([^)]+)\\)").matcher(cellValue);
        return matcher.find() ? matcher.group(1) : null;
    }

    private String extractName(String cellValue) {
        if (cellValue == null) return null;
        int idx = cellValue.indexOf('(');
        return idx > 0 ? cellValue.substring(0, idx).trim() : cellValue.trim();
    }

    private boolean publishFramework(String frameworkId, String authToken, String channelId, XSSFWorkbook wb, File file, int levelCount) {
        StringBuilder strUrl = new StringBuilder(serverProperties.getKmBaseHost());
        strUrl.append(serverProperties.getKmFrameworkPublishPath() + "/" + frameworkId);
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.CONTENT_TYPE, "application/json");
        headers.put(Constants.X_CHANNEL_ID, channelId);

        Map<String, Object> response = outboundRequestHandler.fetchResultUsingPost(strUrl.toString(), new HashMap<>(), headers);
        if (MapUtils.isNotEmpty(response) && "OK".equalsIgnoreCase((String) response.get("responseCode"))) {
            logger.info("Org hierarchy published successfully.");
            return true;
        } else {
            logger.error("Failed to publish org hierarchy: " + response);
            try {
                XSSFSheet sheet = wb.getSheetAt(0);
                int lastRowNum = sheet.getLastRowNum() + 1;
                Row summaryRow = sheet.createRow(lastRowNum);
                Cell cell = summaryRow.createCell(0);
                cell.setCellValue("Publish framework failed");
                Cell errorCell = summaryRow.createCell(levelCount + 1);
                errorCell.setCellValue("Failed to publish org hierarchy: " + (response != null ? response.toString() : "null"));
            } catch (Exception ex) {
                logger.error("Failed to write publish error to Excel: " + ex.getMessage());
            }
            return false;
        }
    }

    private Map<String, Object> findTermInFrameworkByIdentifier(List<Map<String, Object>> frameworkData, String category, String identifier) {
        Map<String, Object> categoryObj = frameworkData.stream()
                .filter(n -> category.equalsIgnoreCase((String) n.get(Constants.NAME)))
                .findFirst().orElse(null);
        logger.info("Searching for identifier: " + identifier + " in category: " + category);
        if (MapUtils.isNotEmpty(categoryObj)) {
            List<Map<String, Object>> terms = (List<Map<String, Object>>) categoryObj.get(Constants.TERMS);
            logger.info("Found " + (terms != null ? terms.size() : 0) + " terms in category: " + category);
            if (CollectionUtils.isNotEmpty(terms)) {
                for (Map<String, Object> t : terms) {
                    Map<String, Object> additionalProps = (Map<String, Object>) t.get(Constants.ADDITIONAL_PROPERTIES);
                    logger.info("Checking term: " + t.get(Constants.NAME) + " with identifier: " + (additionalProps != null ? additionalProps.get(Constants.IDENTIFIER) : "null"));
                    if (MapUtils.isNotEmpty(additionalProps) && identifier.equals(additionalProps.get(Constants.IDENTIFIER))) {
                        return t;
                    }
                }
            }
        }
        return null;
    }

    public String processFrameworkCreate(String masterFramework, String orgId) {
        String fwName = "";
        try {
            logger.info("processFrameworkCreate started");
            Map<String, Object> createReq = createFrameworkRequest(orgId, masterFramework);
            Map<String, Object> request = new HashMap<>();
            request.put(Constants.REQUEST, createReq);
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.X_CHANNEL_ID, orgId);
            StringBuilder strUrl = new StringBuilder(serverProperties.getKnowledgeMS());
            strUrl.append(serverProperties.getFrameworkCopy()).append("/");
            strUrl.append(masterFramework);
            logger.info("Printing URL for copy: {}", strUrl);
            logger.info("Printing request: {}", request);
            Map<String, Object> frameworkResponse = (Map<String, Object>) outboundRequestHandler.fetchResultUsingPost(
                    strUrl.toString(),
                    request, headers);
            if (org.apache.commons.collections.MapUtils.isNotEmpty(frameworkResponse) && Constants.OK.equalsIgnoreCase(
                    (String) frameworkResponse.get(Constants.RESPONSE_CODE))) {
                Map<String, Object> result = (Map<String, Object>) frameworkResponse.get(
                        Constants.RESULT);
                fwName = (String) result.getOrDefault(Constants.NODE_ID, "");
                logger.info("copy framework node id: {}", fwName);
            } else {
                logger.error("Failed to copy the framework: {}",
                        frameworkResponse.get(Constants.RESPONSE_CODE));
            }

        } catch (Exception e) {
            logger.error("Unexpected error occurred in processFrameworkCreate", e);
        }
        return fwName;
    }

    public static Map<String, Object> createFrameworkRequest(String channelId, String masterFramework) {
        Map<String, Object> framework = createFramework(channelId, masterFramework);
        Map<String, Object> request = new HashMap<>();
        request.put("framework", framework);
        return request;
    }

    private static Map<String, Object> createFramework(String channelId, String masterFramework) {
        Map<String, Object> framework = new HashMap<>();
        long time = Instant.now().toEpochMilli();

        StringBuffer name = new StringBuffer();
        name.append(channelId)
                .append("_")
                .append(masterFramework)
                .append("_")
                .append(time);
        framework.put(Constants.NAME, name);
        framework.put(Constants.DESCRIPTION, "Framework for Channel " + channelId + ". This framework is a customized copy derived from the Master Framework");
        framework.put(Constants.CODE, name);
        framework.put(Constants.OWNER, channelId);
        return framework;
    }

    public static Map<String, Object> createOrgHierarchyRequestMap(String organisationId, String frameworkIdKey, String frameworkStatusKey, String frameworkId, String frameworkStatus) {
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.ORGANISATION_ID, organisationId);
        requestMap.put(frameworkIdKey, frameworkId);
        requestMap.put(frameworkStatusKey, frameworkStatus);

        Map<String, Object> outerMap = new HashMap<>();
        outerMap.put(Constants.REQUEST, requestMap);
        return outerMap;
    }

    /**
     * Retires a framework using its ID by invoking the appropriate DELETE endpoint.
     *
     * @param frameworkId The full framework ID (e.g., "01359693287062732810_org_hierarchy")
     * @param userId      The ID of the user invoking the operation
     * @param channelId   The channel ID associated with the framework
     */
    private void retireFramework(String frameworkId, String userId, String channelId) {
        String uri = serverProperties.getLearningServiceVMBaseUrl()
                + serverProperties.getFrameworkRetireEndpointUrl()
                + "/" + frameworkId;

        Map<String, String> headers = new HashMap<>();
        headers.put("user-id", userId);
        headers.put(Constants.X_CHANNEL_ID, channelId);

        try {
            Object rawResponse = outboundRequestHandler.fetchResultUsingDelete(uri, null, headers);

            if (rawResponse instanceof Map) {
                Map<String, Object> response = (Map<String, Object>) rawResponse;
                if (Constants.OK.equals(response.get(Constants.RESPONSE_CODE))) {
                    Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
                    if (result != null) {
                        String nodeId = (String) result.get(Constants.NODE_ID);
                        String versionKey = (String) result.get(Constants.VERSION_KEY);
                        logger.info("Framework retired successfully. Node ID: {}, Version Key: {}", nodeId, versionKey);
                    } else {
                        logger.warn("Framework retired but result object is null. Framework ID: {}", frameworkId);
                    }
                } else {
                    logger.warn("Retire framework call failed. Response: {}", response);
                }
            } else {
                logger.error("Unexpected response type while retiring framework: {}", rawResponse);
            }
        } catch (Exception e) {
            logger.error("Error occurred while retiring framework with ID: {}", frameworkId, e);
        }
    }



}
