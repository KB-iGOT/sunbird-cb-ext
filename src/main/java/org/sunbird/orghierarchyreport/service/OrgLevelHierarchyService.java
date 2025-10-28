package org.sunbird.orghierarchyreport.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface OrgLevelHierarchyService {
    SBApiResponse orgExtSearchV3(Map<String, Object> request);
}
