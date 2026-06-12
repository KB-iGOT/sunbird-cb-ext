package org.sunbird.storage.service;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

import javax.validation.Valid;

public interface StorageService {
	public SBApiResponse uploadFile(MultipartFile file, String containerName) throws IOException;

	SBApiResponse uploadFile(File file, String cloudFolderName, String containerName);

	public SBApiResponse deleteFile(String fileName, String containerName);

    SBApiResponse downloadFile(String fileName);

	ResponseEntity<Resource> downloadFile(String reportType, String date, String orgId, String fileName, String userToken);

	ResponseEntity<Map<String, Map<String, Object>>> getFileInfo(String orgId);
	public SBApiResponse uploadFile(MultipartFile file, String cloudFolderName, String containerName) throws IOException;

	ResponseEntity<?> downloadFile(String reportType, String date, String fileName, String userToken);

	ResponseEntity<?> getFileInfoSpv(String userToken, String date);

	public SBApiResponse downloadFile(String fileName, String containerName);

	SBApiResponse uploadFileForOrg(MultipartFile mFile, String userToken);

	SBApiResponse ciosContentIconUpload(MultipartFile file, String containerName, String cloudFolderName);

	SBApiResponse ciosContentContractUpload(MultipartFile file, String containerName, String cloudFolderName);

	ResponseEntity<?> downloadCiosContractFile(String fileName);

	SBApiResponse uploadCiosLogsFile(String logFilePath,String fileName, String containerName, String cloudFolderName);

	ResponseEntity<?> downloadCiosLogsFile(String fileName);

	/**
	 * Uploads an image to a Google Cloud Platform (GCP) container.
	 *
	 * @param multipartFile the image file to be uploaded
	 * @param requestBody   a map containing the cloud folder and container names
	 * @param authUserToken the authentication token for the user
	 * @return the response object containing the result of the upload operation
	 */
	SBApiResponse uploadImageToGCPContainer(MultipartFile multipartFile, Map<String, Object> requestBody, String authUserToken);

    SBApiResponse uploadAssignmentAnsFile(MultipartFile multipartFile, String contentId, String batchId, String formId);

    ResponseEntity<?> readAssignmentAnsFile(String contentId, String batchId, String formId, String fileName, String userToken);

    ResponseEntity<Resource> downloadFormReport(
            String reportType,
            String date,
            String formId,
            String fileName,
            String userToken);

	/**
	 * Securely uploads a profile photo with validation.
	 *
	 * @param file          the profile photo file to be uploaded
	 * @param cloudFolderName the folder name in cloud storage
	 * @param containerName the container name in cloud storage
	 * @return the response object containing the result of the upload operation
	 */
	SBApiResponse uploadProfilePhoto(MultipartFile file, String cloudFolderName, String containerName);

	ResponseEntity<?> peerValidationReportDownload(Map<String, Object> requestBody, String userToken);

	SBApiResponse uploadUserAchievement(MultipartFile mFile, String cloudFolderName, String containerName);
}
