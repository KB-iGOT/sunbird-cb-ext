package org.sunbird.nlw.service;

import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

/**
 * @author mahesh.vakkund
 */
public interface UserEventPostConsumptionService {
    SBApiResponse processEventUsersForCertificateAndKarmaPoints(MultipartFile multipartFile);
}
