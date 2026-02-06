package org.sunbird.ehrms.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface EhrmsIgotUserSyncService {

    SBApiResponse userEhrmsDataUpdate(Map<String,Object> requestBody, String sync) throws Exception;
    SBApiResponse getSyncStatus();
}
