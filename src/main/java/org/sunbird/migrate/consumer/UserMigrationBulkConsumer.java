package org.sunbird.migrate.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.core.config.PropertiesConfig;
import org.sunbird.migrate.service.impl.UserMigrationServiceImpl;
import org.sunbird.profile.service.ProfileService;
import org.sunbird.storage.service.StorageServiceImpl;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CompletableFuture;

@Component
public class UserMigrationBulkConsumer {

    @Getter
    private static UserMigrationBulkConsumer instance;

    private final Logger logger = LoggerFactory.getLogger(UserMigrationBulkConsumer.class);

    @Autowired
    private UserMigrationServiceImpl userMigrationService;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    private StorageServiceImpl storageService;

    @Autowired
    private ProfileService profileService;

    @Autowired
    private AccessTokenValidator accessTokenValidator;

    @Autowired
    PropertiesConfig configuration;

    private final List<String> messageBuffer = Collections.synchronizedList(new ArrayList<>());

    @PostConstruct
    public void init() {
        instance = this;
    }

    @KafkaListener(topics = "${kafka.topics.org.user.bulk.transfer.event}", groupId = "${kafka.topics.org.user.bulk.transfer.event.group}")
    public void processBulkUserTransferMessage(ConsumerRecord<String, String> data) {
        logger.info("UserMigrationBulkConsumer::processMessage: Received event to initiate Bulk User Transfer Process...");
        logger.info("Received message:: " + data.value());
        try {
            if (StringUtils.isNoneBlank(data.value())) {
                synchronized (messageBuffer) {
                    messageBuffer.add(data.value());
                }
                CompletableFuture.runAsync(() -> {
                    initiateBulkUserTransferProcess(data.value());
                    synchronized (messageBuffer) {
                        messageBuffer.remove(data.value());
                    }
                });
            } else {
                logger.error("Error in Bulk User Transfer Consumer: Invalid Kafka Msg");
            }
        } catch (Exception e) {
            logger.error(String.format("Error in Bulk User Transfer Consumer: Error Msg :%s", e.getMessage()), e);
        }
    }

    public void initiateBulkUserTransferProcess(String message) {
        String identifier = null;
        String rootOrgId = null;

        try {
            Map<String, Object> messageData = objectMapper.readValue(message, Map.class);

            identifier = (String) messageData.get(Constants.IDENTIFIER);
            rootOrgId = (String) messageData.get(Constants.ROOT_ORG_ID);
            String fileName = (String) messageData.get(Constants.FILE_NAME);
            String userAuthToken = (String) messageData.get(Constants.X_AUTH_TOKEN);
            String frameworkId = (String) messageData.get(Constants.FRAMEWORK_ID);

            logger.info("Processing bulk transfer for identifier: {}, rootOrgId: {}", identifier, rootOrgId);

            updateBulkTransferStatus(rootOrgId, identifier, Constants.STATUS_IN_PROGRESS_UPPERCASE, null);

            processExcelWithStatusUpdate(fileName, frameworkId, rootOrgId, userAuthToken, identifier);

        } catch (Exception e) {
            logger.error("Error processing bulk user transfer message: {}", e.getMessage(), e);
            if (StringUtils.isNotEmpty(identifier) && StringUtils.isNotEmpty(rootOrgId)) {
                updateBulkTransferStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, e.getMessage());
            }
        }
    }

    private void processExcelWithStatusUpdate(String fileName, String frameworkId, String rootOrgId, String userAuthToken, String identifier) throws Exception {

        Map<String, Object> frameworkData = userMigrationService.getCompleteFrameworkDataFromUtil(frameworkId);
        Map<String, Set<String>> orgHierarchyMap = buildOrgHierarchyMap(frameworkData);
        Map<String, String> orgIdToChannelMap = buildOrgIdToChannelMap(frameworkData);

        List<String> userRoles = getUserRolesFromToken(userAuthToken);

        storageService.downloadFile(fileName, serverProperties.getOrgHierarchyBulkUploadContainerName());
        File file = new File(Constants.LOCAL_BASE_PATH + fileName);

        if (!file.exists() || file.length() == 0) {
            logger.error("Error processing bulk user transfer:");
            updateBulkTransferFilePathAndStatus(rootOrgId, identifier, null,
                    Constants.FAILED_UPPERCASE, 0, 0, 0, Constants.EMPTY_FILE);
            return;
        }

        int totalRecords = 0;
        int successCount = 0;
        int failedCount = 0;

        FileInputStream fis = null;
        XSSFWorkbook wb = null;

        try {
            fis = new FileInputStream(file);
            wb = new XSSFWorkbook(fis);
            XSSFSheet sheet = wb.getSheetAt(0);

            Row headerRow = sheet.getRow(0);
            if (headerRow == null) {
                logger.error("Error processing bulk user transfer: {}", Constants.HEADER_ROW_NOT_FOUND);
                updateBulkTransferFilePathAndStatus(rootOrgId, identifier, null, Constants.FAILED_UPPERCASE, totalRecords, successCount, failedCount, Constants.HEADER_ROW_NOT_FOUND);
                return;
            }

            int statusColumnIndex = findOrAddColumn(headerRow, Constants.PASCALCASESTATUS);
            int errorColumnIndex = findOrAddColumn(headerRow, Constants.ERRORMESSAGE);

            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next();

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();

                if (isEmptyRow(row)) {
                    continue;
                }

                totalRecords++;
                boolean success = processRowWithStatusUpdate(row, orgHierarchyMap, orgIdToChannelMap, rootOrgId, userAuthToken, userRoles, statusColumnIndex, errorColumnIndex);
                if (success) {
                    successCount++;
                } else {
                    failedCount++;
                }
            }

            uploadTheUpdatedFile(file, wb);
            String finalStatus = failedCount > 0 ? Constants.FAILED_UPPERCASE : Constants.COMPLETED;
            updateBulkTransferFilePathAndStatus(rootOrgId, identifier, null, finalStatus, totalRecords, successCount, failedCount);

        } finally {
            if (wb != null) {
                wb.close();
            }
            if (fis != null) {
                fis.close();
            }
            if (file != null && file.exists()) {
                file.delete();
            }
        }

        logger.info("Bulk transfer completed for identifier: {}. Total: {}, Success: {}, Failed: {}", identifier, totalRecords, successCount, failedCount);
    }

    private Map<String, Set<String>> buildOrgHierarchyMap(Map<String, Object> frameworkData) {
        Map<String, Set<String>> hierarchyMap = new HashMap<>();

        if (MapUtils.isEmpty(frameworkData)) return hierarchyMap;

        Map<String, Object> result = (Map<String, Object>) frameworkData.get(Constants.RESULT);
        if (MapUtils.isEmpty(result)) return hierarchyMap;

        Map<String, Object> framework = (Map<String, Object>) result.get(Constants.FRAMEWORK);
        if (MapUtils.isEmpty(framework)) return hierarchyMap;

        List<Map<String, Object>> categories = (List<Map<String, Object>>) framework.get(Constants.CATEGORIES);
        if (CollectionUtils.isEmpty(categories)) return hierarchyMap;

        Map<String, Set<String>> immediateChildren = new HashMap<>();

        for (Map<String, Object> category : categories) {
            List<Map<String, Object>> terms = (List<Map<String, Object>>) category.get(Constants.TERMS);
            if (CollectionUtils.isEmpty(terms)) continue;

            for (Map<String, Object> term : terms) {
                String parentOrgId = extractOrgIdFromTerm(term);
                if (StringUtils.isEmpty(parentOrgId)) continue;

                Set<String> children = immediateChildren.computeIfAbsent(parentOrgId, k -> new HashSet<>());

                List<Map<String, Object>> associations = (List<Map<String, Object>>) term.get(Constants.ASSOCIATIONS);
                if (CollectionUtils.isNotEmpty(associations)) {
                    for (Map<String, Object> assoc : associations) {
                        String childOrgId = extractOrgIdFromTerm(assoc);
                        if (StringUtils.isNotEmpty(childOrgId)) {
                            children.add(childOrgId);
                        }
                    }
                }
            }
        }

        for (String orgId : immediateChildren.keySet()) {
            Set<String> allDescendants = new HashSet<>();
            buildFullHierarchy(orgId, immediateChildren, allDescendants, new HashSet<>());
            hierarchyMap.put(orgId, allDescendants);
        }

        return hierarchyMap;
    }

    private void buildFullHierarchy(String parentId, Map<String, Set<String>> immediateChildren, Set<String> allDescendants, Set<String> visited) {
        if (visited.contains(parentId)) {
            return;
        }
        visited.add(parentId);

        Set<String> immediateKids = immediateChildren.get(parentId);
        if (CollectionUtils.isNotEmpty(immediateKids)) {
            for (String childId : immediateKids) {
                allDescendants.add(childId);
                buildFullHierarchy(childId, immediateChildren, allDescendants, visited);
            }
        }
        visited.remove(parentId);
    }

    private Map<String, String> buildOrgIdToChannelMap(Map<String, Object> frameworkData) {
        Map<String, String> orgChannelMap = new HashMap<>();

        if (MapUtils.isEmpty(frameworkData)) return orgChannelMap;

        Map<String, Object> result = (Map<String, Object>) frameworkData.get(Constants.RESULT);
        if (MapUtils.isEmpty(result)) return orgChannelMap;

        Map<String, Object> framework = (Map<String, Object>) result.get(Constants.FRAMEWORK);
        if (MapUtils.isEmpty(framework)) return orgChannelMap;

        List<Map<String, Object>> categories = (List<Map<String, Object>>) framework.get(Constants.CATEGORIES);
        if (CollectionUtils.isEmpty(categories)) return orgChannelMap;

        for (Map<String, Object> category : categories) {
            List<Map<String, Object>> terms = (List<Map<String, Object>>) category.get(Constants.TERMS);
            if (CollectionUtils.isEmpty(terms)) continue;

            for (Map<String, Object> term : terms) {
                String orgId = extractOrgIdFromTerm(term);
                String orgName = (String) term.get(Constants.NAME);
                if (StringUtils.isNotEmpty(orgId) && StringUtils.isNotEmpty(orgName)) {
                    orgChannelMap.put(orgId, orgName);
                }

                List<Map<String, Object>> associations = (List<Map<String, Object>>) term.get(Constants.ASSOCIATIONS);
                if (CollectionUtils.isNotEmpty(associations)) {
                    for (Map<String, Object> assoc : associations) {
                        String assocOrgId = extractOrgIdFromTerm(assoc);
                        String assocOrgName = (String) assoc.get(Constants.NAME);
                        if (StringUtils.isNotEmpty(assocOrgId) && StringUtils.isNotEmpty(assocOrgName)) {
                            orgChannelMap.put(assocOrgId, assocOrgName);
                        }
                    }
                }
            }
        }

        return orgChannelMap;
    }

    private String extractOrgIdFromTerm(Map<String, Object> term) {
        Map<String, Object> additionalProperties = (Map<String, Object>) term.get(Constants.ADDITIONAL_PROPERTIES);
        return MapUtils.isNotEmpty(additionalProperties) ? (String) additionalProperties.get(Constants.ORG_ID) : null;
    }

    private List<String> getUserRolesFromToken(String userAuthToken) {
        try {
            Map<String, Object> tokenPayload = accessTokenValidator.extractTokenPayload(userAuthToken);
            List<String> userRoles = (List<String>) tokenPayload.get(Constants.USER_ROLES_KEY);
            return userRoles;
        } catch (Exception e) {
            logger.error("Error extracting user roles from token", e);
            return new ArrayList<>();
        }
    }

    private boolean processRowWithStatusUpdate(Row row, Map<String, Set<String>> orgHierarchyMap, Map<String, String> orgIdToChannelMap, String rootOrgId, String userAuthToken, List<String> userRoles, int statusColumnIndex, int errorColumnIndex) {
        String emailId = "";
        String currentOrgName = "";
        String targetOrgName = "";
        String notifyUser = "";

        try {
            emailId = getCellStringValue(row.getCell(0));
            currentOrgName = getCellStringValue(row.getCell(1));
            targetOrgName = getCellStringValue(row.getCell(2));
            notifyUser = getCellStringValue(row.getCell(3));

            if (StringUtils.isAnyBlank(emailId, currentOrgName, targetOrgName)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.MISSING_REQUIRED_FIELDS);
                return false;
            }

            String currentOrgId = extractOrgIdFromName(currentOrgName);
            String targetOrgId = extractOrgIdFromName(targetOrgName);

            if (!currentOrgId.equalsIgnoreCase(rootOrgId)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.ORGANIZATION_MISMATCH);
                return false;
            }

            if (!orgIdToChannelMap.containsKey(currentOrgId)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.CURRENT_ORG_NOT_FOUND);
                return false;
            }

            if (!orgIdToChannelMap.containsKey(targetOrgId)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.TARGET_ORG_NOT_FOUND);
                return false;
            }

            if (!isValidTransferBasedOnRole(userRoles, currentOrgId, targetOrgId, orgHierarchyMap)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.TRANSFER_NOT_ALLOWED);
                return false;
            }

            Map<String, Object> userDetails = getUserByEmailAndOrg(emailId, currentOrgId);
            if (MapUtils.isEmpty(userDetails)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.USER_NOT_FOUND_IN_CURRENT_ORG);
                return false;
            }

            String userId = (String) userDetails.get(Constants.USER_ID);
            String targetChannel = orgIdToChannelMap.get(targetOrgId);
            String expectedTargetOrgName = extractOrgNameFromInput(targetOrgName);

            if (!targetChannel.equalsIgnoreCase(expectedTargetOrgName)) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, String.format(Constants.TARGET_ORG_NAME_MISMATCH, expectedTargetOrgName, targetChannel));
                return false;
            }

            boolean success = processUserTransfer(userId, targetChannel, Constants.TRUE.equalsIgnoreCase(notifyUser), userAuthToken);

            if (success) {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.SUCCESS_UPPERCASE, "");
                return true;
            } else {
                updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, Constants.USER_TRANSFER_FAILED);
                return false;
            }

        } catch (Exception e) {
            String errorMessage = String.format(Constants.ERROR_PROCESSING_TRANSFER, emailId, e.getMessage());
            logger.error(errorMessage, e);
            updateRowStatus(row, statusColumnIndex, errorColumnIndex, Constants.FAILED_UPPERCASE, errorMessage);
            return false;
        }
    }

    private void updateRowStatus(Row row, int statusColumnIndex, int errorColumnIndex, String status, String errorMessage) {
        Cell statusCell = row.getCell(statusColumnIndex);
        if (statusCell == null) {
            statusCell = row.createCell(statusColumnIndex);
        }
        statusCell.setCellValue(status);

        Cell errorCell = row.getCell(errorColumnIndex);
        if (errorCell == null) {
            errorCell = row.createCell(errorColumnIndex);
        }
        errorCell.setCellValue(errorMessage);
    }

    private boolean isValidTransferBasedOnRole(List<String> userRoles, String currentOrgId, String targetOrgId, Map<String, Set<String>> orgHierarchyMap) {
        if (userRoles.contains(Constants.MDO_ADMIN)) {
            return isForwardPathInHierarchy(currentOrgId, targetOrgId, orgHierarchyMap);
        }

        if (userRoles.contains(Constants.MDO_LEADER) || userRoles.contains(Constants.SPV_ADMIN) || userRoles.contains(Constants.STATE_ADMIN)) {
            return isValidOrgInFramework(targetOrgId, orgHierarchyMap);
        }

        return false;
    }

    private boolean isForwardPathInHierarchy(String currentOrgId, String targetOrgId, Map<String, Set<String>> orgHierarchyMap) {
        Set<String> visited = new HashSet<>();
        return findForwardPath(currentOrgId, targetOrgId, orgHierarchyMap, visited);
    }

    private boolean findForwardPath(String currentOrgId, String targetOrgId, Map<String, Set<String>> orgHierarchyMap, Set<String> visited) {
        if (visited.contains(currentOrgId)) {
            return false;
        }
        visited.add(currentOrgId);

        Set<String> children = orgHierarchyMap.get(currentOrgId);
        if (CollectionUtils.isEmpty(children)) {
            return false;
        }

        // Direct child case
        if (children.contains(targetOrgId)) {
            return true;
        }

        // Check forward path only
        for (String childId : children) {
            if (findForwardPath(childId, targetOrgId, orgHierarchyMap, visited)) {
                return true;
            }
        }

        return false;
    }

    private boolean isInSameHierarchyPath(String currentOrgId, String targetOrgId, Map<String, Set<String>> orgHierarchyMap) {
        Set<String> currentOrgPath = new HashSet<>();
        buildCompletePath(currentOrgId, orgHierarchyMap, currentOrgPath);

        return currentOrgPath.contains(targetOrgId) || isAncestor(targetOrgId, currentOrgId, orgHierarchyMap);
    }

    private void buildCompletePath(String orgId, Map<String, Set<String>> orgHierarchyMap, Set<String> path) {
        Set<String> children = orgHierarchyMap.get(orgId);
        if (CollectionUtils.isEmpty(children)) {
            return;
        }

        path.addAll(children);
        for (String childId : children) {
            buildCompletePath(childId, orgHierarchyMap, path);
        }
    }

    private boolean isAncestor(String possibleAncestor, String currentOrgId, Map<String, Set<String>> orgHierarchyMap) {
        for (Map.Entry<String, Set<String>> entry : orgHierarchyMap.entrySet()) {
            if (entry.getKey().equals(possibleAncestor) && entry.getValue().contains(currentOrgId)) {
                return true;
            }
        }
        return false;
    }

    private boolean processUserTransfer(String userId, String targetChannel, boolean notifyUser, String userAuthToken) {
        try {
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put(Constants.USER_ID_CONSTANT, userId);
            requestBody.put(Constants.CHANNEL, targetChannel);
            requestBody.put(Constants.FORCE_MIGRATION, true);
            requestBody.put(Constants.SOFT_DELETE_OLD_ORG, false);
            requestBody.put(Constants.NOTIFY_MIGRATION, notifyUser);

            Map<String, Object> request = new HashMap<>();
            request.put(Constants.REQUEST, requestBody);

            SBApiResponse migrateResponse = profileService.migrateUser(request, userAuthToken, serverProperties.getSbApiKey());

            if ((migrateResponse != null && Constants.SUCCESS.equalsIgnoreCase((String) migrateResponse.get(Constants.RESPONSE)))) {
                logger.info("User migration successful: userId={}, targetChannel={}", userId, targetChannel);
                return true;
            } else {
                logger.error("User migration failed: userId={}, response={}", userId, migrateResponse);
                return false;
            }
        } catch (Exception e) {
            logger.error("Error in user transfer: userId={}, targetChannel={}", userId, targetChannel, e);
            return false;
        }
    }

    private int findOrAddColumn(Row headerRow, String columnName) {
        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            if (cell != null && columnName.equals(cell.getStringCellValue())) {
                return i;
            }
        }
        int newColumnIndex = headerRow.getLastCellNum();
        Cell newCell = headerRow.createCell(newColumnIndex);
        newCell.setCellValue(columnName);
        return newColumnIndex;
    }

    private boolean isEmptyRow(Row row) {
        for (int i = 0; i < 4; i++) {
            Cell cell = row.getCell(i);
            if (cell != null && cell.getCellType() != CellType.BLANK && StringUtils.isNotBlank(cell.toString())) {
                return false;
            }
        }
        return true;
    }

    private String getCellStringValue(Cell cell) {
        if (cell == null) return "";
        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue().trim();
            case NUMERIC:
                return String.valueOf((long) cell.getNumericCellValue());
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            default:
                return "";
        }
    }

    private String extractOrgIdFromName(String orgName) {
        if (StringUtils.isNotEmpty(orgName) && orgName.contains("(") && orgName.endsWith(")")) {
            int startIndex = orgName.lastIndexOf("<") + 1;
            int endIndex = orgName.lastIndexOf(">");
            return orgName.substring(startIndex, endIndex).trim();
        }
        return orgName;
    }

    private Map<String, Object> getUserByEmailAndOrg(String emailId, String orgId) {
        try {
            Map<String, Object> filters = new HashMap<>();
            filters.put(Constants.ROOT_ORG_ID, orgId);
            filters.put(Constants.PROFILE_DETAILS_PRIMARY_EMAIL, emailId);
            filters.put(Constants.STATUS, 1);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put(Constants.FILTERS, filters);

            Map<String, Object> request = new HashMap<>();
            request.put(Constants.REQUEST, requestBody);

            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.AUTHORIZATION, serverProperties.getSbApiKey());
            Map<String, Object> searchResponse = outboundRequestHandlerService.fetchResultUsingPost(configuration.getLmsServiceHost() + configuration.getLmsUserSearchEndPoint(), request, null);

            if (searchResponse != null && Constants.OK.equalsIgnoreCase((String) searchResponse.get(Constants.RESPONSE_CODE))) {
                Map<String, Object> result = (Map<String, Object>) searchResponse.get(Constants.RESULT);
                if (result != null) {
                    Map<String, Object> response = (Map<String, Object>) result.get(Constants.RESPONSE);
                    if (response != null) {
                        List<Map<String, Object>> content = (List<Map<String, Object>>) response.get(Constants.CONTENT);

                        if (CollectionUtils.isNotEmpty(content)) {
                            Map<String, Object> user = content.get(0);
                            String userRootOrgId = (String) user.get(Constants.ROOT_ORG_ID);

                            if (orgId.equals(userRootOrgId)) {
                                return user;
                            }
                        }
                    }
                }
            }

            return null;

        } catch (Exception e) {
            logger.error("Error fetching user via search API: emailId={}, orgId={}", emailId, orgId, e);
            return null;
        }
    }

    private String uploadTheUpdatedFile(File file, XSSFWorkbook wb) throws IOException {
        FileOutputStream fileOut = new FileOutputStream(file);
        wb.write(fileOut);
        fileOut.close();
        SBApiResponse uploadResponse = storageService.uploadFile(file, serverProperties.getOrgHierarchyBulkUploadContainerName(), serverProperties.getCloudContainerName());
        if (!HttpStatus.OK.equals(uploadResponse.getResponseCode())) {
            logger.info(String.format(Constants.FAILED_TO_UPLOAD_FILE, uploadResponse.getParams().getErrmsg()));
            return Constants.FAILED_UPPERCASE;
        }
        return Constants.SUCCESSFUL_UPPERCASE;
    }

    private void updateBulkTransferStatus(String rootOrgId, String identifier, String status, String comment) {
        updateBulkTransferFilePathAndStatus(rootOrgId, identifier, null, status, 0, 0, 0, comment);
    }

    private void updateBulkTransferFilePathAndStatus(String rootOrgId, String identifier, String filePath, String status, int totalRecords, int successCount, int failedCount) {
        updateBulkTransferFilePathAndStatus(rootOrgId, identifier, filePath, status, totalRecords, successCount, failedCount, null);
    }

    private void updateBulkTransferFilePathAndStatus(String rootOrgId, String identifier, String filePath, String status, int totalRecords, int successCount, int failedCount, String comment) {
        try {
            Map<String, Object> updateData = new HashMap<>();
            updateData.put(Constants.STATUS, status);
            updateData.put(Constants.DATE_UPDATE_ON, new Timestamp(System.currentTimeMillis()));

            if (StringUtils.isNotEmpty(filePath)) {
                updateData.put(Constants.FILE_PATH, filePath);
            }
            if (StringUtils.isNotEmpty(comment)) {
                updateData.put(Constants.COMMENT, comment);
            }
            if (totalRecords > 0) {
                updateData.put(Constants.TOTAL_RECORDS, totalRecords);
                updateData.put(Constants.SUCCESSFUL_RECORDS_COUNT, successCount);
                updateData.put(Constants.FAILED_RECORDS_COUNT, failedCount);
            }

            Map<String, Object> compositeKey = new HashMap<>();
            compositeKey.put(Constants.ROOT_ORG_ID, rootOrgId);
            compositeKey.put(Constants.IDENTIFIER, identifier);

            cassandraOperation.updateRecord(Constants.KEYSPACE_SUNBIRD, Constants.ORG_USER_BULK_TRANSFER_TABLE, updateData, compositeKey);
        } catch (Exception e) {
            logger.error("Error updating bulk transfer status: identifier={}, status={}", identifier, status, e);
        }
    }

    @PreDestroy
    public void shutdownHook() {
        logger.info("Shutdown hook triggered. Processing buffered messages...");
        synchronized (messageBuffer) {
            for (String message : messageBuffer) {
                try {
                    logger.info("Processing buffered message: {}", message);
                    updateDBStatusAtShutDown(message);
                    logger.info("Successfully processed message during shutdown: {}", message);
                } catch (Exception e) {
                    logger.error("Error processing message during shutdown: {}", message, e);
                }
            }
            messageBuffer.clear();
        }
        logger.info("Shutdown hook completed.");
    }

    public void updateDBStatusAtShutDown(String message) {
        try {
            Map<String, Object> messageData = objectMapper.readValue(message, Map.class);
            String identifier = (String) messageData.get(Constants.IDENTIFIER);
            String rootOrgId = (String) messageData.get(Constants.ROOT_ORG_ID);
            updateBulkTransferStatus(rootOrgId, identifier, Constants.FAILED_UPPERCASE, Constants.PROCESS_INTERRUPTED_DURING_SHUTDOWN);
        } catch (Exception e) {
            logger.error("Error updating status during shutdown: {}", e.getMessage(), e);
        }
    }

    private String extractOrgNameFromInput(String orgNameInput) {
        if (StringUtils.isNotEmpty(orgNameInput) && orgNameInput.contains("<") && orgNameInput.endsWith(">")) {
            int startIndex = orgNameInput.lastIndexOf("<");
            return orgNameInput.substring(0, startIndex).trim();
        }
        return orgNameInput.trim();
    }

    private boolean isValidOrgInFramework(String targetOrgId, Map<String, Set<String>> orgHierarchyMap) {
        if (orgHierarchyMap.containsKey(targetOrgId)) {
            return true;
        }
        for (Set<String> children : orgHierarchyMap.values()) {
            if (children.contains(targetOrgId)) {
                return true;
            }
        }
        return false;
    }
}