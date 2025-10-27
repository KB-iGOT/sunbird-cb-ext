package org.sunbird.orghierarchyreport.service;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.org.model.OrgHierarchy;
import org.sunbird.org.model.OrgHierarchyInfo;
import org.sunbird.org.service.ExtendedOrgServiceImpl;
import org.sunbird.orghierarchyreport.repository.MdoChildrenLookUpRepository;

import java.util.*;

@Service
public class OrgHierarchyReportServiceImpl implements OrgHierarchyReportService {

    private final MdoChildrenLookUpRepository mdoChildrenLookupRepository;

    private final CbExtServerProperties configProperties;

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    private final ExtendedOrgServiceImpl extendedOrgService;

    public OrgHierarchyReportServiceImpl(MdoChildrenLookUpRepository mdoChildrenLookupRepository, CbExtServerProperties configProperties, OutboundRequestHandlerServiceImpl outboundRequestHandlerService, ExtendedOrgServiceImpl extendedOrgService) {
        this.mdoChildrenLookupRepository = mdoChildrenLookupRepository;
        this.configProperties = configProperties;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.extendedOrgService = extendedOrgService;
    }

    @Override
    public SBApiResponse orgExtSearchV3(Map<String, Object> request) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_ORG_HIERACHY_SEARCH_V3);
        Map<String, Object> searchFilters = new HashMap<>();
        String errMsg = validateSearchRequest(request, searchFilters);
        if (StringUtils.isNotBlank(errMsg)) {
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            response.getParams().setErrmsg(errMsg);
            return response;
        }
        List<String> indentifiersList = (List<String>) searchFilters.get(Constants.IDENTIFIER);
        List<String> childrenIdsList = mdoChildrenLookupRepository.findAllChildrenByMdoId(indentifiersList);
        if (CollectionUtils.isEmpty(childrenIdsList)) {
            return handleNoChildrenScenario(request);
        }
        handleChildrenScenario(childrenIdsList, response);
        return response;
    }

    private void handleChildrenScenario(List<String> childrenIdsList, SBApiResponse response) {
        String childrenIds = childrenIdsList.get(0);
        List<String> orgIds = Arrays.asList(childrenIds.split(","));
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put(Constants.OFFSET, 0);
        requestMap.put(Constants.LIMIT, 1000);
        Map<String, Object> sortByMap = new HashMap<>();
        sortByMap.put(Constants.CHANNEL, Constants.ASC_ORDER);
        requestMap.put(Constants.SORT_BY, sortByMap);
        Map<String, Object> filters = new HashMap<>();
        filters.put(Constants.IS_TENANT, Boolean.TRUE);
        filters.put(Constants.IDENTIFIER, orgIds);
        requestMap.put(Constants.FILTERS, filters);
        String serviceURL = configProperties.getSbUrl() + configProperties.getSbOrgSearchPath();
        Map<String, Object> payload = new HashMap<>();
        payload.put(Constants.REQUEST, requestMap);
        Map<String, Object> orgResponse = (Map<String, Object>) outboundRequestHandlerService.fetchResultUsingPost(serviceURL, payload);
        Map<String, Object> responseMap = (Map<String, Object>) orgResponse.get(Constants.RESULT);
        List<Map<String, Object>> mappedList = buildMappedOrgList(responseMap);
        response.put(Constants.RESPONSE, mappedList);
    }

    private SBApiResponse handleNoChildrenScenario(Map<String, Object> request) {
        SBApiResponse orgSearchResponse = extendedOrgService.orgExtSearchV2(request);
        Map<String, Object> result = orgSearchResponse.getResult();
        List<OrgHierarchyInfo> responseList = (List<OrgHierarchyInfo>) result.get(Constants.RESPONSE);
        SBApiResponse listAllOrgResponse = extendedOrgService.listAllOrg(responseList.get(0).getMapId());
        result = listAllOrgResponse.getResult();
        Map<String, Object> responseMap = (Map<String, Object>) result.get(Constants.RESPONSE);
        List<OrgHierarchy> contentList = (List<OrgHierarchy>) responseMap.get(Constants.CONTENT);
        cleanOrgHierarchyList(contentList);
        Map<String, Object> contentMap = new HashMap<>();
        contentMap.put(Constants.CONTENT, contentList);
        contentMap.put(Constants.COUNT, contentList.size());
        listAllOrgResponse.put(Constants.RESPONSE, contentMap);
        return listAllOrgResponse;
    }

    private void cleanOrgHierarchyList(List<OrgHierarchy> content) {
        for (OrgHierarchy org : content) {
            org.setId(null);
            org.setMapId("");
            org.setOrgCode("");
            org.setParentMapId("");
            org.setSbRootOrgId("");
            org.setSbOrgType("");
            org.setSbOrgSubType("");
            org.setL1MapId("");
            org.setL2MapId(null);
            org.setL1OrgName("");
            org.setL2OrgName(null);
        }
    }

    private List<Map<String, Object>> buildMappedOrgList(Map<String, Object> responseMap) {
        Map<String, Object> responseData = (Map<String, Object>) responseMap.get(Constants.RESPONSE);
        List<Map<String, Object>> contentList = (List<Map<String, Object>>) responseData.get(Constants.CONTENT);
        List<Map<String, Object>> mappedList = new ArrayList<>();
        for (Map<String, Object> org : contentList) {
            Map<String, Object> mapped = new HashMap<>();
            mapped.put(Constants.ID, "");
            mapped.put(Constants.ORG_NAME, org.getOrDefault(Constants.ORG_NAME, ""));
            mapped.put(Constants.CHANNEL, org.getOrDefault(Constants.CHANNEL, ""));
            mapped.put(Constants.MAP_ID, "");
            mapped.put(Constants.ORG_CODE, "");
            mapped.put(Constants.PARENT_MAP_ID, "");
            mapped.put(Constants.SB_ORG_ID, org.getOrDefault("id", ""));
            mapped.put(Constants.SB_ROOT_ORG_ID, "");
            mapped.put(Constants.SB_ORG_TYPE, "");
            mapped.put(Constants.SB_SUB_ORG_TYPE, "");
            mapped.put(Constants.L1_MAP_ID, "");
            mapped.put(Constants.L2_MAP_ID, "");
            mapped.put(Constants.L1_ORG_NAME, "");
            mapped.put(Constants.L2_ORG_NAME, "");
            mappedList.add(mapped);
        }
        return mappedList;
    }


    private String validateSearchRequest(Map<String, Object> request, Map<String, Object> searchFilters) {
        String errMsg = "";
        Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
        if (ObjectUtils.isEmpty(requestBody)) {
            errMsg = Constants.INVALID_REQUEST;
            return errMsg;
        }
        Map<String, Object> filters = (Map<String, Object>) requestBody.get(Constants.FILTERS);
        if (ObjectUtils.isEmpty(filters)) {
            errMsg = Constants.INVALID_REQUEST;
            return errMsg;
        }

        boolean filterExist = false;
        if (filters.containsKey(Constants.IDENTIFIER)) {
            filterExist = true;
            List<String> orgList = (List<String>) filters.get(Constants.IDENTIFIER);
            if (CollectionUtils.isNotEmpty(orgList)) {
                searchFilters.put(Constants.IDENTIFIER, orgList);
            } else {
                errMsg = "Identifier list is empty";
            }
        }

        if (filters.containsKey(Constants.ORG_NAME) && filters.containsKey(Constants.PARENT_TYPE)) {
            filterExist = true;
            errMsg = validateOrgName(searchFilters, filters, errMsg);

            Object parentTypeValue = filters.get(Constants.PARENT_TYPE);
            errMsg = validateParentType(searchFilters, parentTypeValue, errMsg);
        }

        if (!filterExist) {
            errMsg = "Need identifier OR orgName and parentType in Filters";
        }
        validateLimit(searchFilters, requestBody);
        return errMsg;
    }

    private void validateLimit(Map<String, Object> searchFilters, Map<String, Object> requestBody) {
        Integer limit = (Integer) requestBody.get(Constants.LIMIT);
        if (limit == null) {
            searchFilters.put(Constants.LIMIT, configProperties.getOrgSearchResponseDefaultLimit());
        } else {
            searchFilters.put(Constants.LIMIT, limit);
        }
    }

    private String validateParentType(Map<String, Object> searchFilters, Object parentTypeValue, String errMsg) {
        if (parentTypeValue instanceof List<?>) {
            List<?> parentTypeList = (List<?>) parentTypeValue;
            if (!parentTypeList.isEmpty() && !(parentTypeList.size() == 1 && "".equals(parentTypeList.get(0))) && parentTypeList.stream().allMatch(String.class::isInstance)) {
                searchFilters.put(Constants.PARENT_TYPE, parentTypeList);
            } else {
                errMsg = "ParentType is empty in search request";
            }
        } else if (parentTypeValue instanceof String) {
            String parentTypeStr = ((String) parentTypeValue).trim();
            if (StringUtils.isNotBlank(parentTypeStr)) {
                searchFilters.put(Constants.PARENT_TYPE, parentTypeStr);
            } else {
                errMsg = "ParentType is empty in search request";
            }
        } else {
            errMsg = "Invalid data type for ParentType";
        }
        return errMsg;
    }

    private String validateOrgName(Map<String, Object> searchFilters, Map<String, Object> filters, String errMsg) {
        if (StringUtils.isNotBlank((String) filters.get(Constants.ORG_NAME))) {
            searchFilters.put(Constants.ORG_NAME, ((String) filters.get(Constants.ORG_NAME)).trim());
        } else {
            errMsg = "OrgName is empty in search request.";
        }
        return errMsg;
    }
}
