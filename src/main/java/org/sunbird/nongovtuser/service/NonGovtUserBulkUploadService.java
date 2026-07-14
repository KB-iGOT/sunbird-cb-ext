package org.sunbird.nongovtuser.service;

import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

/**
 * Bulk creation API for Non-Govt (Volunteer) users.
 * <p>
 * Volunteer Onboarding (MDO/SPV Portal): a SPV/MDO admin onboards volunteers in bulk
 * and the platform notifies them with login details.
 */
public interface NonGovtUserBulkUploadService {

    /**
     * Entry point for the non-govt user bulk upload request.
     *
     * @param mFile         uploaded CSV of volunteer records
     * @param orgId         SPV/MDO admin's org id (must resolve to a Volunteer Organisation)
     * @param userId        id of the admin performing the upload
     * @param userAuthToken caller's auth token
     */
    SBApiResponse bulkUploadNonGovtUsers(MultipartFile mFile, String orgId,
                                          String userId, String userAuthToken, String targetOrgId);
}
