package org.sunbird.orghierarchyreport.service;

import org.sunbird.common.model.SBApiResponse;

import java.util.Map;

public interface OrgHierarchyReportService {
    SBApiResponse orgExtSearchV3(Map<String, Object> request);
}
