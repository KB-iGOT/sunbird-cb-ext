package org.sunbird.storage.service;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.cloud.storage.BaseStorageService;
import org.sunbird.cloud.storage.factory.StorageConfig;
import org.sunbird.cloud.storage.factory.StorageServiceFactory;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import scala.Option;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileOutputStream;

@Service
public class PeerValidationFileServiceImpl implements PeerValidationFileService {

    private Logger logger = LoggerFactory.getLogger(getClass().getName());
    private BaseStorageService storageService = null;

    @Autowired
    private CbExtServerProperties serverProperties;

    @Autowired
    private StorageService storageServiceImpl;

    @PostConstruct
    public void init() {
        if (storageService == null) {
            storageService = StorageServiceFactory.getStorageService(new StorageConfig(
                    serverProperties.getCloudStorageTypeName(), serverProperties.getCloudStorageKey(),
                    serverProperties.getCloudStorageSecret().replace("\\n", "\n"), Option.apply(serverProperties.getCloudStorageEndpoint()), Option.empty()));
        }
    }

    @Override
    public SBApiResponse uploadPeerValidationFile(MultipartFile mFile, String formId, String userId) {

        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_FILE_UPLOAD);
        File file = null;

        try {

            SBApiResponse validation = validateFile(mFile, response);

            if (validation != null) {
                return validation;
            }

            String extension = FilenameUtils.getExtension(mFile.getOriginalFilename());

            String smartFileName =
                    formId + "_" + userId + "_" + System.currentTimeMillis() + "." + extension;

            file = new File(smartFileName);
            file.createNewFile();

            try (FileOutputStream fos = new FileOutputStream(file)) {
                fos.write(mFile.getBytes());
            }

            String cloudFolderName =
                    serverProperties.getPeerValidationCloudFolderName()
                            + Constants.SLASH
                            + formId
                            + Constants.SLASH
                            + userId;

            return storageServiceImpl.uploadFile(file, cloudFolderName, serverProperties.getCloudPublicContainerName());

        } catch (Exception e) {

            logger.error("Failed to upload peer validation file", e);

            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to upload file. Exception: " + e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);

            return response;

        } finally {

            if (file != null && file.exists()) {
                file.delete();
            }
        }
    }

    private SBApiResponse validateFile(MultipartFile file, SBApiResponse response) {

        if (file == null || file.isEmpty()) {
            ProjectUtil.returnErrorMsg( "File cannot be empty", HttpStatus.BAD_REQUEST, response, String.valueOf(HttpStatus.BAD_REQUEST));
            return response;
        }

        String contentType = file.getContentType();
        long size = file.getSize();

        if (contentType == null) {
            ProjectUtil.returnErrorMsg( "Invalid file type", HttpStatus.BAD_REQUEST, response, String.valueOf(HttpStatus.BAD_REQUEST));
            return response;
        }

        if ("application/pdf".equalsIgnoreCase(contentType)) {

            if (size > serverProperties.getPeerValidationPdfMaxSize()) {
                ProjectUtil.returnErrorMsg( "PDF size should not exceed allowed limit", HttpStatus.BAD_REQUEST, response, String.valueOf(HttpStatus.BAD_REQUEST));
                return response;
            }

        } else if ("video/mp4".equalsIgnoreCase(contentType)) {

            if (size > serverProperties.getPeerValidationVideoMaxSize()) {
                ProjectUtil.returnErrorMsg( "Video size should not exceed allowed limit", HttpStatus.BAD_REQUEST, response, String.valueOf(HttpStatus.BAD_REQUEST));
                return response;
            }

        } else {
            ProjectUtil.returnErrorMsg( "Only PDF and MP4 files are allowed", HttpStatus.BAD_REQUEST, response, String.valueOf(HttpStatus.BAD_REQUEST));
            return response;
        }

        return null;
    }

}
