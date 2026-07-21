package org.sunbird.nongovtuser.controller;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.nongovtuser.service.NonGovtUserBulkUploadService;

/**
 * Bulk creation API for Non-Govt (Volunteer) users.
 * Request/header shape cloned from
 * {@link org.sunbird.profile.controller.ProfileController#bulkUploadV2}.
 * <p>
 */
@RequiredArgsConstructor
@RestController
public class NonGovtUserController {

    private final NonGovtUserBulkUploadService nonGovtUserBulkUploadService;

    @PostMapping(Constants.NON_GOVT_USER_BULK_UPLOAD_ENDPOINT)
    public ResponseEntity<SBApiResponse> bulkUploadNonGovtUsers(
            @RequestParam(value = Constants.FILE, required = true) MultipartFile multipartFile,
            @RequestHeader(value = Constants.TARGET_ORG_ID, required = true) String targetOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId,
            @RequestHeader(Constants.X_AUTH_USER_ID) String userId,
            @RequestHeader(Constants.X_AUTH_TOKEN) String userAuthToken) throws UnsupportedEncodingException {
        SBApiResponse uploadResponse = nonGovtUserBulkUploadService.bulkUploadNonGovtUsers(
                multipartFile, rootOrgId, userId, userAuthToken, targetOrgId);
        return new ResponseEntity<>(uploadResponse, uploadResponse.getResponseCode());
    }
}
