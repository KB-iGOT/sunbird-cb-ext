package org.sunbird.ehrms.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface EhrmsIgotUserSyncService {

    SBApiResponse userEhrmsDataUpdate(Map<String,Object> requestBody) throws Exception;
    SBApiResponse getSyncStatus(Map<String, Object> requestBody);
}
