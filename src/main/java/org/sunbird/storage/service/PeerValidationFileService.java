package org.sunbird.storage.service;

import org.springframework.web.multipart.MultipartFile;
import org.sunbird.common.model.SBApiResponse;

public interface PeerValidationFileService {

     SBApiResponse uploadPeerValidationFile(MultipartFile mFile, String formId, String userToken);
}
