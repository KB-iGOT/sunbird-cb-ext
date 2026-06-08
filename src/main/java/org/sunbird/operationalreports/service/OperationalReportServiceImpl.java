package org.sunbird.operationalreports.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.model.enums.EncryptionMethod;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.Model;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.config.PropertiesConfig;
import org.sunbird.operationalreports.exception.ZipProcessingException;
import org.sunbird.org.model.OrgHierarchy;
import org.sunbird.org.repository.OrgHierarchyRepository;
import org.sunbird.org.service.ExtendedOrgService;
import org.sunbird.user.service.UserUtilityService;
import scala.Option;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * @author Deepak kumar Thakur & Mahesh R V
 */
@Service
public class OperationalReportServiceImpl implements OperationalReportService {
    private static final Logger logger = LoggerFactory.getLogger(OperationalReportServiceImpl.class);

    private static final String ALPHANUMERIC_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    @Autowired
    CbExtServerProperties serverProperties;

    @Autowired
    PropertiesConfig configuration;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundReqService;

    @Autowired
    OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    private CassandraOperation cassandraOperation;

    @Autowired
    private CbExtServerProperties serverConfig;

    @Autowired
    private UserUtilityService userUtilityService;

    private BaseStorageService storageService = null;

    @Autowired
    OrgHierarchyRepository orgHierarchyRepository;

    @Autowired
    private RedisCacheMgr redisCacheMgr;

    private Storage storage;

    private PrivateKey privateKey;

    @Autowired
    private ExtendedOrgService extendedOrgService;

    @Override
    public SBApiResponse grantReportAccessToMDOAdmin(Map<String, Object> requestBody, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.GRANT_REPORT_ACCESS_API);
        try {
            String mdoLeaderUserId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            if (StringUtils.isBlank(mdoLeaderUserId)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Failed to read user details from access token.");
                return response;
            }
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            String mdoAdminUserId = (String) request.get(Constants.USER_ID);
            String reportExpiryDateRequest = (String) request.get(Constants.REPORT_EXPIRY_DATE);
            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Arrays.asList(mdoLeaderUserId, mdoAdminUserId),
                    Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID), userInfoMap);
            String mdoLeaderOrgId = userInfoMap.get(mdoLeaderUserId).get(Constants.ROOT_ORG_ID);
            String mdoAdminOrgId = userInfoMap.get(mdoAdminUserId).get(Constants.ROOT_ORG_ID);
            if (!mdoLeaderOrgId.equalsIgnoreCase(mdoAdminOrgId)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Requested User is not belongs to same organisation.");
                return response;
            }
            Map<String, Object> mdoAdminData = userUtilityService.getUsersReadData(mdoAdminUserId, null, null);
            List<String> mdoAdminRoles = (List<String>) mdoAdminData.get(Constants.ROLES);
            if (!mdoAdminRoles.contains(Constants.MDO_ADMIN)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Requested User is not MDO_ADMIN.");
                return response;
            }
            if (!mdoAdminRoles.contains(Constants.MDO_REPORT_ACCESSOR)) {
                mdoAdminRoles.add(Constants.MDO_REPORT_ACCESSOR);
                Map<String, Object> assignRoleReq = new HashMap<>();
                Map<String, Object> roleRequestBody = new HashMap<>();
                roleRequestBody.put(Constants.ORGANIZATION_ID, mdoLeaderOrgId);
                roleRequestBody.put(Constants.USER_ID, mdoAdminUserId);
                roleRequestBody.put(Constants.ROLES, mdoAdminRoles);
                assignRoleReq.put(Constants.REQUEST, roleRequestBody);

                Map<String, Object> assignRoleResp = outboundRequestHandlerService.fetchResultUsingPost(
                        serverConfig.getSbUrl() + serverConfig.getSbAssignRolePath(), assignRoleReq,
                        null);
                if (!Constants.OK.equalsIgnoreCase((String) assignRoleResp.get(Constants.RESPONSE_CODE))) {
                    logger.error("Failed to assign MDO_REPORT_ACCESSOR role for user. Response : %s",
                            (new ObjectMapper()).writeValueAsString(assignRoleResp));
                    response.getParams().setStatus(Constants.FAILED);
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                    response.getResult().put(Constants.MESSAGE, "Failed to assign MDO_REPORT_ACCESSOR role for user.");
                    return response;
                }
            }

            Map<String, Object> dbResponse = upsertReportAccessExpiry(mdoAdminUserId, mdoLeaderOrgId,
                    reportExpiryDateRequest);
            if (Constants.SUCCESS.equalsIgnoreCase((String) dbResponse.get(Constants.RESPONSE))) {
                response.getResult().put(Constants.STATUS, Constants.SUCCESS);
            } else {
                response.getParams().setStatus(Constants.FAILED);
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                response.getResult().put(Constants.MESSAGE, "Failed to update DB record.");
            }
        } catch (Exception e) {
            logger.error("An exception occurred {}", e.getMessage(), e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;
    }

    @PostConstruct
    public void init() {
        if (storageService == null) {
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(
                    serverProperties.getCloudStorageTypeName(), serverProperties.getCloudStorageKey(),
                    serverProperties.getCloudStorageSecret().replace("\\n", "\n"),
                    Option.apply(serverProperties.getCloudStorageEndpoint()), Option.empty()));
        }

        try {
            privateKey = loadPrivateKey(serverProperties.getJwtPrivateKeyPath());
            logger.info("JWT private key initialized.");

        } catch (Exception e) {
            logger.error("Failed to initialize JWT private key: {}", e.getMessage(), e);
            throw new RuntimeException("Service initialization failed", e);
        }
    }

    @Override
    public ResponseEntity<InputStreamResource> downloadFile(String authToken) throws Exception {
        HttpHeaders headers = new HttpHeaders();
        String sourceFolderPath = null;
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            if (null == userId) {
                throw new Exception("User Id does not exist");
            }
            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Collections.singletonList(userId), Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID, Constants.CHANNEL),
                    userInfoMap);
            Map<String, String> userDetailsMap = userInfoMap.get(userId);
            String rootOrg = userDetailsMap.get(Constants.ROOT_ORG_ID);

            String channel = userDetailsMap.get(Constants.CHANNEL);
            InputStreamResource inputStreamResource = null;
            String reportFileName = "";
            sourceFolderPath = String.format("%s/%s/%s/", Constants.LOCAL_BASE_PATH, rootOrg, UUID.randomUUID());
            String outputPath = sourceFolderPath +  Constants.OUTPUT_PATH;
            if (serverProperties.getSpvChannelName().equalsIgnoreCase(channel)) {
                logger.info("This is under spv: " + channel);
                reportFileName = serverProperties.getSpvFullReportFileName();
                String objectKey = serverProperties.getReportDownloadFolderName() + "/" + serverProperties.getSpvFullReportReportFolderName() + "/"
                        + serverProperties.getSpvFullReportFileName();
                inputStreamResource = getInputStreamForZip(reportFileName, objectKey, headers, sourceFolderPath);
            } else {
                logger.info("This is under mdo: " + channel + " rootOrgId: " + rootOrg);
                reportFileName = serverProperties.getOperationReportFileName();
                String objectKey = serverProperties.getOperationalReportFolderName() + "/mdoid=" + rootOrg + "/"
                        + serverProperties.getOperationReportFileName();
                inputStreamResource = getInputStreamForZip(reportFileName, objectKey, headers, sourceFolderPath);
            }

            // Return ResponseEntity with the file for download
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(Files.size(Paths.get(outputPath + "/" + serverProperties.getOperationReportFileName())))
                    .body(inputStreamResource);
        } catch (Exception e) {
            logger.error("Failed to read the downloaded file: " + serverProperties.getOperationReportFileName()
                    + ", Exception: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }  finally {
            if (sourceFolderPath != null) {
                try {
                    removeDirectory(String.valueOf(Paths.get(sourceFolderPath)));
                } catch (InvalidPathException e) {
                    logger.error("Failed to delete the file: " + sourceFolderPath + ", Exception: ", e);
                }
            }
        }
    }

    /**
     * Creates a zip folder from the files in the specified source folder path.
     *
     * @param sourceFolderPath The path of the source folder.
     * @param fileName         The name of the zip file to be created.
     * @param password         The password for encrypting the zip file.
     */
    public static void createZipFolder(String sourceFolderPath, String fileName, String password) {
        // Initialize an ArrayList to store files to be added to the zip folder
        ArrayList<File> filesToAdd = new ArrayList<>();
        // Retrieve all files from the source folder
        getAllFiles(new File(sourceFolderPath + "/unzippath"), filesToAdd);
        try (ZipFile zipFile = new ZipFile(sourceFolderPath + "/" + fileName)) {
            // Configure zip parameters
            ZipParameters parameters = new ZipParameters();
            if (password != null && !password.isEmpty()) {
                // Set password and encryption method if password is provided
                zipFile.setPassword(password.toCharArray());
                parameters.setEncryptFiles(true);
                parameters.setEncryptionMethod(EncryptionMethod.ZIP_STANDARD);
            }
            // Add files to the zip folder
            zipFile.addFiles(filesToAdd, parameters);
            logger.info("Zip folder created successfully.");
        } catch (IOException e) {
            // Throw exception if an error occurs during zip folder creation
            throw new ZipProcessingException("An error occurred while creating zip folder");
        }
    }

    /**
     * Unlocks a password-protected zip folder and extracts its contents to the
     * specified destination folder path.
     *
     * @param zipFilePath           The path of the password-protected zip folder.
     * @param destinationFolderPath The path where the contents of the zip folder
     *                              will be extracted.
     * @param password              The password for unlocking the zip folder
     */
    public static void unlockZipFolder(String zipFilePath, String destinationFolderPath, String password) {
        try (ZipFile zipFile = new ZipFile(zipFilePath)) {
            // Check if the zip folder is encrypted and a password is provided
            if (zipFile.isEncrypted() && (password != null && !password.isEmpty())) {
                // Set password for decryption
                zipFile.setPassword(password.toCharArray());
            }
            // Extract contents of the zip folder to the destination folder
            zipFile.extractAll(destinationFolderPath);
            logger.info("Zip folder unlocked successfully.");
        } catch (IOException e) {
            // Throw exception if an error occurs during unlocking of the zip folder
            throw new ZipProcessingException("IO Exception while unlocking the zip folder");
        } finally {
            if (zipFilePath != null) {
                try {
                    removeDirectory(String.valueOf(Paths.get(zipFilePath)));
                } catch (InvalidPathException e) {
                    logger.error("Failed to delete the file: " + zipFilePath + ", Exception: ", e);
                }
            }
        }
    }

    /**
     * Recursively traverses a directory and its subdirectories to collect all
     * files.
     *
     * @param dir      The directory to start traversing from.
     * @param fileList The list to which the files will be added.
     */
    public static void getAllFiles(File dir, List<File> fileList) {
        // Get the list of files in the current directory
        File[] files = dir.listFiles();
        // If the directory is not empty
        if (files != null) {
            // Iterate through each file in the directory
            for (File file : files) {
                // If the file is a directory, recursively call getAllFiles to traverse it
                if (file.isDirectory()) {
                    getAllFiles(file, fileList);
                } else { // If the file is a regular file, add it to the fileList
                    fileList.add(file);
                }
            }
        }
    }

    /**
     * Recursively removes a directory and all its contents.
     *
     * @param directoryPath The path of the directory to be removed.
     */
    public static void removeDirectory(String directoryPath) {
        // Get the Path object for the specified directory path
        Path path = Paths.get(directoryPath);
        try (Stream<Path> pathStream = Files.walk(path)) {
            // Traverse all paths in the directory in reverse order to delete inner files
            // first
            pathStream
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            // Attempt to delete each path (file or directory)
                            Files.delete(p);
                        } catch (IOException e) {
                            logger.info("Failed to delete: " + p + " - " + e.getMessage());
                        }
                    });
            // Delete the directory itself if it still exists
            Files.deleteIfExists(path);
            logger.info("Directory removed successfully.");
        } catch (IOException e) {
            logger.info("Failed to remove directory: " + e.getMessage());
        }
    }

    /**
     * Generates a random alphanumeric password of the specified length.
     *
     * @param length The length of the password to generate.
     * @return A randomly generated alphanumeric password.
     */
    public static String generateAlphanumericPassword(int length) {
        // Initialize SecureRandom for generating random numbers
        SecureRandom random = new SecureRandom();
        // StringBuilder to construct the password
        StringBuilder sb = new StringBuilder(length);
        // Loop to generate random characters
        for (int i = 0; i < length; i++) {
            // Generate a random index within the range of alphanumeric characters
            int randomIndex = random.nextInt(ALPHANUMERIC_CHARACTERS.length());
            // Append a randomly selected character to the StringBuilder
            sb.append(ALPHANUMERIC_CHARACTERS.charAt(randomIndex));
        }
        // Return the generated password
        return sb.toString();
    }

    private Map<String, Object> getSearchObject(List<String> userIds) {
        Map<String, Object> requestObject = new HashMap<>();
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.USER_ID, userIds);
        request.put(Constants.LIMIT, userIds.size());
        request.put(Constants.OFFSET, 0);
        request.put(Constants.FILTERS, filters);
        request.put(Constants.FIELDS_CONSTANT,
                Arrays.asList(Constants.USER_ID, Constants.STATUS, Constants.CHANNEL, Constants.ROOT_ORG_ID));
        requestObject.put(Constants.REQUEST, request);
        return requestObject;
    }

    private long getParsedDate(String reportExpiryDate) throws ParseException {
        long reportExpiryDateMillis;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date parsedDate = dateFormat.parse(reportExpiryDate);
        reportExpiryDateMillis = parsedDate.getTime();
        return reportExpiryDateMillis;
    }

    public SBApiResponse getFileInfo(String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.GET_FILE_INFO_OPERATIONAL_REPORTS);
        try {
            logger.info("Inside the getFileInfo()");
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            if (null == userId) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("User Id does not exist");
                return response;
            }
            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Collections.singletonList(userId), Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID),
                    userInfoMap);
            Map<String, String> userDetailsMap = userInfoMap.get(userId);
            String rootOrg = userDetailsMap.get(Constants.ROOT_ORG_ID);
            String objectKey = serverProperties.getOperationalReportFolderName() + "/mdoid=" + rootOrg + "/"
                    + serverProperties.getOperationReportFileName();
            logger.info("Object key for the operational Reports : " + objectKey);
            Model.Blob blob = storageService.getObject(serverProperties.getReportDownloadContainerName(), objectKey,
                    Option.apply(Boolean.FALSE));
            if (blob != null) {
                logger.info("File details" + blob.lastModified());
                logger.info("File details" + blob.metadata());
                response.put(Constants.LAST_MODIFIED, blob.lastModified());
                response.put(Constants.FILE_METADATA, blob.metadata());
            }
        } catch (Exception e) {
            logger.error("Failed to get the report file information. Exception: ", e);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to get the report file information");
            return response;
        }
        return response;
    }

    public SBApiResponse readGrantAccess(String authToken, boolean isAdminAPI) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.READ_REPORT_ACCESS_API);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
        if (StringUtils.isBlank(userId)) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to read user details from access token.");
            return response;
        }
        Map<String, Map<String, String>> userInfoMap = new HashMap<>();
        userUtilityService.getUserDetailsFromDB(
                Arrays.asList(userId),
                Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID), userInfoMap);
        String userOrgId = userInfoMap.get(userId).get(Constants.ROOT_ORG_ID);
        if (StringUtils.isBlank(userOrgId)) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Requested User is not belongs to same organisation.");
            return response;
        }

        Map<String, Object> primaryKeyMap = new HashMap<>();
        primaryKeyMap.put(Constants.ORG_ID, userOrgId);
        if (isAdminAPI) {
            primaryKeyMap.put(Constants.USER_ID, userId);
        }
        Map<String, Object> userAccessReportExpiryDetails = getReportAccessDetails(primaryKeyMap);
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, Object> entry : userAccessReportExpiryDetails.entrySet()) {
            Map<String, Object> mdoIdAccessDetails = new HashMap<>();
            mdoIdAccessDetails.put(Constants.USER_ID, entry.getKey());
            mdoIdAccessDetails.put(Constants.REPORT_ACCESS_EXPIRY,
                    ((Map<String, Object>) entry.getValue()).get(Constants.USER_REPORT_EXPIRY_DATE));
            result.add(mdoIdAccessDetails);
        }
        response.getResult().put(Constants.STATUS, Constants.SUCCESS);
        response.getResult().put(Constants.RESPONSE, result);
        return response;
    }

    /**
     * @param rootOrgId 
     * @param authToken
     * @param requestBody
     * @return
     */
    @Override
    public ResponseEntity<InputStreamResource> downloadIndividualReport(String rootOrgId, String authToken, Map<String, Object> requestBody) {
        HttpHeaders headers = new HttpHeaders();
        String sourceFolderPath = null;
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            InputStreamResource inputStreamResource = null;
            if (StringUtils.isBlank(userId)) {
                throw new Exception("Failed to read user details from access token.");
            }
            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Collections.singletonList(userId), Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID, Constants.CHANNEL),
                    userInfoMap);
            Map<String, String> userDetailsMap = userInfoMap.get(userId);
            String rootOrg = userDetailsMap.get(Constants.ROOT_ORG_ID);
            logger.info("the rootOrg for user is:" + rootOrg + " the rootOrgId is: " + rootOrgId);
            if (StringUtils.isNotBlank(rootOrg) && StringUtils.isNotBlank(rootOrgId)) {
                if (!rootOrg.equalsIgnoreCase(rootOrgId)) {
                    throw new Exception("the org is not proper.");
                }
            } else {
                throw new Exception("the org is not proper.");
            }
            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            if (MapUtils.isEmpty(request)) {
                throw new Exception("RequestBody is not proper.");
            }
            List<String> childIds = (List<String>) request.get(Constants.CHILD_ID);
            List<String> orgIds = new ArrayList<>();
            orgIds.add(rootOrgId);
            List<OrgHierarchy> orgHierarchyList = orgHierarchyRepository.findAllBySbOrgId(orgIds);
            String mapId = "";
            if (CollectionUtils.isNotEmpty(orgHierarchyList)) {
                if (orgHierarchyList.get(0) != null) {
                    mapId = orgHierarchyList.get(0).getMapId();
                }
            }

            if (StringUtils.isBlank(mapId) && CollectionUtils.isNotEmpty((childIds))) {
                throw new Exception("Issue while fetching orgHierarchy for orgId: " + rootOrgId);
            }
            String reportFileName = serverProperties.getOperationReportFileName();
            sourceFolderPath = String.format("%s/%s/%s/", Constants.LOCAL_BASE_PATH, rootOrgId, UUID.randomUUID());
            String outputPath = sourceFolderPath +  Constants.OUTPUT_PATH;
            for (String childId : childIds) {
                //boolean isChildPresent = orgHierarchyRepository.isChildOrgPresent(mapId, childId);
               // if (isChildPresent) {
                    logger.info("This is under mdo: " + childId + " rootOrgId: " + rootOrgId);
                    String objectKey = serverProperties.getOperationalReportFolderName() + "/mdoid=" + childId + "/"
                            + serverProperties.getOperationReportFileName();
                    createTheZipAndStoreForOrg(reportFileName, objectKey, sourceFolderPath);
                // } else {
                //     logger.error("ChildId is not proper for orgId: " + rootOrgId);
                //     if (childIds.size() == 1) {
                //         throw new Exception("ChildId is not proper for orgId:: " + rootOrgId);
                //     }
                // }
            }
            if (CollectionUtils.isEmpty(childIds)) {
                String objectKey = serverProperties.getOperationalReportFolderName() + "/mdoid=" + rootOrgId + "/"
                        + serverProperties.getOperationReportFileName();
                createTheZipAndStoreForOrg(reportFileName, objectKey, sourceFolderPath);
            }
            inputStreamResource = getPasswordProtectedInputStream(serverProperties.getOperationReportFileName(),  headers, outputPath, rootOrgId);
            return ResponseEntity.ok()
                    .headers(headers)
                    .contentLength(Files.size(Paths.get(outputPath + "/" + serverProperties.getOperationReportFileName())))
                    .body(inputStreamResource);
        } catch (Exception e) {
            logger.error("Failed to read the downloaded file: " + serverProperties.getOperationReportFileName()
                    + ", Exception: ", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        } finally {
            if (sourceFolderPath != null) {
                try {
                    removeDirectory(String.valueOf(Paths.get(sourceFolderPath)));
                } catch (InvalidPathException e) {
                    logger.error("Failed to delete the file: " + sourceFolderPath + ", Exception: ", e);
                }
            }
        }

    }

    private Map<String, Object> upsertReportAccessExpiry(String mdoAdminUserId, String rootOrgId,
            String reportExpiryDate)
            throws ParseException {
        Map<String, Object> primaryKeyMap = new HashMap<>();
        primaryKeyMap.put(Constants.USER_ID_LOWER, mdoAdminUserId);
        primaryKeyMap.put(Constants.ORG_ID, rootOrgId);
        Date reportExpiryDateMillis = parseDateFromString(reportExpiryDate);
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put(Constants.USER_REPORT_EXPIRY_DATE, reportExpiryDateMillis);
        return cassandraOperation.updateRecord(
                Constants.KEYSPACE_SUNBIRD, Constants.REPORT_ACCESS_EXPIRY_TABLE, keyMap, primaryKeyMap);
    }

    private Map<String, Object> getReportAccessDetails(Map<String, Object> primaryKeyMap) {
        return cassandraOperation.getRecordsByPropertiesByKey(Constants.KEYSPACE_SUNBIRD,
                Constants.REPORT_ACCESS_EXPIRY_TABLE, primaryKeyMap,
                Arrays.asList(Constants.USER_REPORT_EXPIRY_DATE, Constants.USER_ID), Constants.USER_ID);
    }

    private Date parseDateFromString(String dateString) {
        OffsetDateTime offsetDateTime = OffsetDateTime.parse(dateString, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        return Date.from(offsetDateTime.toInstant());
    }

    private InputStreamResource getInputStreamForZip(String fileName, String objectKey, HttpHeaders headers, String sourceFolderPath) throws IOException {
        logger.info("The fileName is" + fileName);
        logger.info("The ObjectKey is" + objectKey);
        storageService.download(serverProperties.getReportDownloadContainerName(), objectKey,
                sourceFolderPath + Constants.INPUT_PATH, Option.apply(Boolean.FALSE));
        // Set the file path
        Path filePath = Paths.get(sourceFolderPath + Constants.INPUT_PATH + fileName);
        // Set headers for the response
        headers.add(HttpHeaders.CONTENT_DISPOSITION,
                "attachment; filename=\"" + fileName + "\"");
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        // Prepare password for encryption

        String password = getZipProtectPassword();
        headers.add(Constants.PASSWORD, password);
        // Unzip the downloaded file
        String destinationFolderPath = sourceFolderPath + Constants.OUTPUT_PATH + Constants.UNZIP_PATH;
        String zipFilePath = String.valueOf(filePath);
        unlockZipFolder(zipFilePath, destinationFolderPath, serverProperties.getUnZipFilePassword());
        // Encrypt the unzipped files and create a new zip file
        createZipFolder(sourceFolderPath + Constants.OUTPUT_PATH, fileName, password);
        // Prepare InputStreamResource for the file to be downloaded
        return new InputStreamResource(Files
                .newInputStream(Paths.get(sourceFolderPath + Constants.OUTPUT_PATH + "/" + fileName)));
    }

    private void createTheZipAndStoreForOrg(String fileName, String objectKey, String sourceFolderPath) {
        try {
            logger.info("The fileName is" + fileName);
            logger.info("The ObjectKey is" + objectKey);
            storageService.download(serverProperties.getReportDownloadContainerName(), objectKey,
                    sourceFolderPath + Constants.INPUT_PATH, Option.apply(Boolean.FALSE));
            // Set the file path
            Path filePath = Paths.get(sourceFolderPath + Constants.INPUT_PATH + fileName);

            String destinationFolderPath = sourceFolderPath + Constants.OUTPUT_PATH + Constants.UNZIP_PATH;
            String zipFilePath = String.valueOf(filePath);
            unlockZipFolder(zipFilePath, destinationFolderPath, serverProperties.getUnZipFilePassword());
        } catch (Exception e) {
            logger.error("The data is not present for the objectKey:" + objectKey, e);
        }
    }

    private InputStreamResource getPasswordProtectedInputStream(String fileName, HttpHeaders headers, String sourceFolderPath, String rootOrgId) throws Exception {
        try {
            logger.info("The fileName is" + fileName);
            logger.info("The sourceFolderPath is" + sourceFolderPath + "rootOrgId: " + rootOrgId);
            headers.add(HttpHeaders.CONTENT_DISPOSITION,
                    "attachment; filename=\"" + fileName + "\"");
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            // Prepare password for encryption

            String password = redisCacheMgr.getCache(rootOrgId + Constants.UNDER_SCORE + Constants.PASSWORD);
            if (StringUtils.isNotBlank(password)) {
                headers.add(Constants.PASSWORD, password);
            } else {
                password = getZipProtectPassword();
                headers.add(Constants.PASSWORD, password);
                redisCacheMgr.putStringInCache(rootOrgId + Constants.UNDER_SCORE + Constants.PASSWORD, password, (int)serverConfig.getCacheMaxTTL());
            }

            // Encrypt the unzipped files and create a new zip file
            createZipFolder(sourceFolderPath, fileName, password);
            // Prepare InputStreamResource for the file to be downloaded
            return new InputStreamResource(Files
                    .newInputStream(Paths.get(sourceFolderPath + "/" + fileName)));
        } catch(Exception e) {
            logger.error("Issue while generating the file source Folder path:" + sourceFolderPath, e);
            throw new Exception("Not able to get the input Stream");
        }
    }

    private String getZipProtectPassword() {
        int passwordLength = serverProperties.getZipFilePasswordLength();
        return generateAlphanumericPassword(passwordLength);
    }

    @Override
    public ResponseEntity<Map<String, Object>> operationalReportDownloadV3(String rootOrgId, String authToken, Map<String, Object> requestBody, HttpServletRequest servletRequest) {

        Map<String, Object> response = new HashMap<>();
        try {
            String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);
            if (StringUtils.isBlank(userId)) {
                return buildErrorResponse("Invalid or missing user token", HttpStatus.UNAUTHORIZED);
            }

            Map<String, Map<String, String>> userInfoMap = new HashMap<>();
            userUtilityService.getUserDetailsFromDB(
                    Collections.singletonList(userId),
                    Arrays.asList(Constants.ROOT_ORG_ID, Constants.USER_ID),
                    userInfoMap
            );

            Map<String, String> userInfo = userInfoMap.get(userId);
            if (MapUtils.isEmpty(userInfo)) {
                return buildErrorResponse("User details not found", HttpStatus.NOT_FOUND);
            }

            String userRootOrg = userInfo.get(Constants.ROOT_ORG_ID);
            if (StringUtils.isBlank(userRootOrg) || !userRootOrg.equalsIgnoreCase(rootOrgId)) {
                return buildErrorResponse("User does not have access to this organization",
                        HttpStatus.FORBIDDEN);
            }

            Map<String, Object> request = (Map<String, Object>) requestBody.get(Constants.REQUEST);
            if (MapUtils.isEmpty(request)) {
                return buildErrorResponse("Invalid request body", HttpStatus.BAD_REQUEST);
            }

            List<String> childIds = (List<String>) request.get(Constants.CHILD_ID);
            String targetId = CollectionUtils.isNotEmpty(childIds) ? childIds.get(0) : rootOrgId;
            Map<String, Object> cassandraResponse = extendedOrgService.getOrgDetailsFromDB(targetId);
            if (MapUtils.isEmpty(cassandraResponse)) {
                return buildErrorResponse("Invalid orgId", HttpStatus.BAD_REQUEST);
            }
            String targetOrgName = (String) cassandraResponse.get(Constants.ORG_NAME_LOWERCASE);
            if (StringUtils.isBlank(targetOrgName)) {
                return buildErrorResponse("Invalid orgId", HttpStatus.BAD_REQUEST);
            }
            String password = redisCacheMgr.getCache(targetId + Constants.UNDER_SCORE + Constants.PASSWORD);

            if (StringUtils.isBlank(password)) {
                return buildErrorResponse("Report not available", HttpStatus.NOT_FOUND);
            }

            String objectKey =
                    serverProperties.getOperationalReportFolderName()
                            + "/mdoid="
                            + targetId
                            + "/"
                            + serverProperties.getOperationReportFileName();

            String container = serverProperties.getReportDownloadContainerName();

            Model.Blob blobMetadata = storageService.getObjectOrNull(container, objectKey, Option.apply(false));

            if (ObjectUtils.isEmpty(blobMetadata)) {
                return buildErrorResponse("Report not found", HttpStatus.NOT_FOUND);
            }
            String clientIp = getClientIp(servletRequest);

            String ticket = generateDownloadTicket(userId, container, objectKey, clientIp, targetId, targetOrgName);
            String edgeDownloadUrl = serverProperties.getCbDownloadProxyBaseUrl() + "/download/v3/operational/report/" + targetId + "?t=" + ticket;
            response.put(Constants.STATUS, Constants.SUCCESS);
            response.put(Constants.DOWNLOAD_URL, edgeDownloadUrl);
            response.put(Constants.PASSWORD, password);
            response.put(Constants.FILE_NAME, serverProperties.getOperationReportFileName());
            response.put(Constants.CONTENT_LENGTH, blobMetadata.contentLength());
            response.put("ticketExpiresInMinutes", serverProperties.getOperationalReportJwtTTL() / 60);

            return ResponseEntity.ok(response);

        } catch (Exception e) {
            logger.error("Failed to generate download URL.", e);
            return buildErrorResponse("Internal server error", HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private String generateDownloadTicket(String userId, String bucketName, String objectKey, String clientIp, String targetId, String targetOrgName) {

        long now = System.currentTimeMillis();
        String jti = UUID.randomUUID().toString();
        String sid = UUID.randomUUID().toString().replace("-", "");
        int jwtTtlSeconds = serverProperties.getOperationalReportJwtTTL();
        String token = Jwts.builder()
                .setId(jti)
                .setAudience("igot-edge")
                .claim("uid", userId)
                .claim("bkt", bucketName)
                .claim("obj", objectKey)
                .claim("ip", clientIp)
                .claim("sid", sid)
                .claim("fid", targetId)
                .claim("fname",targetOrgName)
                .setSubject(userId)
                .setIssuedAt(new Date(now))
                .setNotBefore(new Date(now - 5000))
                .setIssuer("igot-backend")
                .setHeaderParam("kid", "report-download-key")
                .setExpiration(new Date(now + (jwtTtlSeconds * 1000L)))
                .signWith(SignatureAlgorithm.RS256, privateKey)
                .compact();
        redisCacheMgr.putStringInCache("dl:session:" + userId, sid, jwtTtlSeconds + 30);
        redisCacheMgr.putStringInCache("dl:ticket:" + jti, "1", jwtTtlSeconds);
        return token;
    }

    private PrivateKey loadPrivateKey(String privateKeyPath) {
        try {
            String key = new String(Files.readAllBytes(Paths.get(privateKeyPath)));
            key = key.replace("-----BEGIN PRIVATE KEY-----", "")
                    .replace("-----END PRIVATE KEY-----", "")
                    .replaceAll("\\s+", "");

            byte[] decoded = Base64.getDecoder().decode(key);
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(decoded);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(spec);

        } catch (Exception e) {
            logger.error("Failed to load private key from path {}. Error: {}", privateKeyPath, e.getMessage(), e);
            throw new RuntimeException("Failed to load private key from " + privateKeyPath, e);
        }
    }

    private ResponseEntity<Map<String, Object>> buildErrorResponse(String message, HttpStatus status) {
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.STATUS, Constants.FAILED);
        response.put(Constants.RESPONSE_CODE, status.value());
        response.put(Constants.MESSAGE, message);
        return ResponseEntity.status(status).body(response);
    }

    private String getClientIp(HttpServletRequest request) {
        String xForwardedFor = request.getHeader("X-Forwarded-For");
        if (StringUtils.isNotBlank(xForwardedFor)) {
            return xForwardedFor.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }
}
