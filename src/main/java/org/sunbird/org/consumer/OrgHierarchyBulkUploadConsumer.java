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
import org.sunbird.storage.service.StorageService;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;

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
            String generateCode
    ) {
        Map<String, Object> requestBody = new HashMap<>();
        Map<String, Object> termReq = new HashMap<>();
        termReq.put(Constants.CODE, generateCode);
        termReq.put(Constants.CATEGORY, category);
        termReq.put(Constants.NAME, extractName(name));
        Map<String, Object> additionalProperties = new HashMap<>();
        additionalProperties.put(Constants.IDENTIFIER, extractIdentifier(name));
        additionalProperties.put(Constants.PARENT_ORG_NAME, parentOrgName);
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
        SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getCompetencyDesignationBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.info(String.format("Failed to upload file. Error: %s", uploadResponse.getParams().getErrmsg()));
            return Constants.FAILED_UPPERCASE;
        }
        return Constants.SUCCESSFUL_UPPERCASE;
    }

    private void processBulkHierarchyUpload(HashMap<String, String> inputDataMap) throws Exception {
        File file = new File(Constants.LOCAL_BASE_PATH + inputDataMap.get(Constants.FILE_NAME));
        FileInputStream fis = new FileInputStream(file);
        XSSFWorkbook wb = new XSSFWorkbook(fis);
        XSSFSheet sheet = wb.getSheetAt(0);

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
        Cell statusHeaderCell = headerRow.createCell(lastHeaderCellNum);
        statusHeaderCell.setCellValue("Status");
        Cell errorHeaderCell = headerRow.createCell(lastHeaderCellNum + 1);
        errorHeaderCell.setCellValue("Error");

        String frameworkId = inputDataMap.get(Constants.FRAMEWORK_ID);
        String orgId = inputDataMap.get(Constants.ROOT_ORG_ID);

        Iterator<Row> rowIterator = sheet.iterator();
        if (rowIterator.hasNext()) rowIterator.next();

        List<Map<String, Object>> frameworkData = getMasterCompetencyFrameworkData(frameworkId);

        int levelCount = 10;

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
                processHierarchyRow(levels, categories, frameworkId, orgId, frameworkData, errors, row, wb, file, levelCount);
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

    private void processHierarchyRow(List<String> levels, List<String> categories, String frameworkId, String orgId,
                                     List<Map<String, Object>> frameworkData, List<String> errors, Row row, XSSFWorkbook wb, File file, int levelCount) throws Exception {
        String parentTermId = null;
        String parentCategory = null;

        try {
            List<Map<String, Object>> createdTerms = new ArrayList<>();
            for (int i = 0; i < levels.size(); i++) {
                String levelName = levels.get(i);
                String category = categories.get(i);

                if (StringUtils.isBlank(levelName)) {
                    errors.add("Level L" + (i + 1) + " is blank");
                    break;
                }
                logger.info("Processing level: " + levelName + " in category: " + category);
                Map<String, Object> term = findTermInFramework(frameworkData, category, levelName);
                if (MapUtils.isEmpty(term)) {
                    String parentOrgName = (i > 0) ? levels.get(i - 1) : null;
                    String generateCode = UUIDs.timeBased().toString();
                    Map<String, Object> createReq = buildCreateTermRequest(frameworkId, levelName, category, parentTermId, parentOrgName, generateCode);
                    Map<String, Object> createResp = createFrameworkTerm(frameworkId, createReq, category);
                    if (MapUtils.isNotEmpty(createResp) && createResp.containsKey(Constants.NODE_ID)) {
                        term = new HashMap<>();
                        term.put(Constants.IDENTIFIER,((List<String>) createResp.get(Constants.NODE_ID)).get(0));
                        term.put(Constants.CODE, generateCode);
                        term.put(Constants.NAME, levelName);
                    } else {
                        errors.add("Failed to create term for " + levelName);
                        break;
                    }
                } else {
                    logger.info("Term already exists for category: " + category + ", levelName: " + levelName);
                }

                createdTerms.add(term);

                if (i > 0) {
                    Map<String, Object> parentTerm = createdTerms.get(i - 1);
                    String parentCode = (String) parentTerm.get(Constants.CODE);

                    Map<String, Object> parentTermInFramework = findTermInFramework(frameworkData, categories.get(i - 1), (String) parentTerm.get(Constants.NAME));
                    List<Map<String, Object>> associations = parentTermInFramework != null && parentTermInFramework.get(Constants.ASSOCIATIONS) != null
                            ? new ArrayList<>((List<Map<String, Object>>) parentTermInFramework.get(Constants.ASSOCIATIONS))
                            : new ArrayList<>();

                    Map<String, Object> assoc = new HashMap<>();
                    assoc.put(Constants.IDENTIFIER, term.get(Constants.IDENTIFIER));

                    boolean exists = false;
                    for (Map<String, Object> a : associations) {
                        if (term.get(Constants.IDENTIFIER).equals(a.get(Constants.IDENTIFIER))) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) {
                        associations.add(assoc);
                    }

                    Map<String, Object> updateReq = updateRequestObject(associations);
                    Map<String, Object> updateResp = updateFrameworkTerm(frameworkId, updateReq, categories.get(i - 1), parentCode);
                    if (MapUtils.isEmpty(updateResp)) {
                        errors.add("Failed to associate " + levelName + " with parent");
                        break;
                    }
                }

                parentTermId = (String) term.get(Constants.NAME);
                parentCategory = category;
            }
        } catch (Exception e) {
            errors.add("Exception: " + e.getMessage());
        }

        Cell statusCell = row.getCell(levelCount, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
        Cell errorCell = row.getCell(levelCount + 1, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
        if (errors.isEmpty()) {
            statusCell.setCellValue(Constants.SUCCESS_UPPERCASE);
            errorCell.setCellValue("");
        } else {
            statusCell.setCellValue(Constants.FAILED_UPPERCASE);
            errorCell.setCellValue(String.join(", ", errors));
        }
        saveWorkbook(wb, file);
    }

    private void saveWorkbook(XSSFWorkbook wb, File file) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(file);
        wb.write(fileOut);
        fileOut.close();
    }

    private Map<String, Object> findTermInFramework(List<Map<String, Object>> frameworkData, String category, String name) {
        String identifier = extractIdentifier(name);
        logger.info("Extracted identifier: " + identifier);
        if (identifier != null) {
            Map<String, Object> termById = findTermInFrameworkByIdentifier(frameworkData, category, identifier);
            if (termById != null) {
                return termById;
            }
        }
        Map<String, Object> categoryObj = frameworkData.stream()
                .filter(n -> category.equalsIgnoreCase((String) n.get(Constants.NAME)))
                .findFirst().orElse(null);
        if (MapUtils.isNotEmpty(categoryObj)) {
            List<Map<String, Object>> terms = (List<Map<String, Object>>) categoryObj.get(Constants.TERMS);
            if (terms != null) {
                String cleanName = extractName(name);
                return terms.stream()
                        .filter(t -> cleanName.equalsIgnoreCase((String) t.get(Constants.NAME)))
                        .findFirst().orElse(null);
            }
        }
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
        String masterDataTerms = redisCacheMgr.getCache(Constants.ORG_MASTER_DATA + "_" + frameworkId);
        if (StringUtils.isEmpty(masterDataTerms)) {
            List<Map<String, Object>> masterData = populateDataFromFrameworkTerm(frameworkId);
            redisCacheMgr.putCache(Constants.ORG_MASTER_DATA + "_" + frameworkId, masterData, serverProperties.getRedisMasterDataReadTimeOut());
            return masterData;
        } else {
            return objectMapper.readValue(masterDataTerms, new TypeReference<List<Map<String, Object>>>() {});
        }
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
        if (MapUtils.isNotEmpty(categoryObj)) {
            List<Map<String, Object>> terms = (List<Map<String, Object>>) categoryObj.get(Constants.TERMS);
            if (CollectionUtils.isNotEmpty(terms)) {
                for (Map<String, Object> t : terms) {
                    Map<String, Object> additionalProps = (Map<String, Object>) t.get(Constants.ADDITIONAL_PROPERTIES);
                    if (MapUtils.isNotEmpty(additionalProps) && identifier.equals(additionalProps.get(Constants.IDENTIFIER))) {
                        return t;
                    }
                }
            }
        }
        return null;
    }

}
