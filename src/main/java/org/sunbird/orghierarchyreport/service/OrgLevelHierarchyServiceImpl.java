package org.sunbird.orghierarchyreport.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.org.model.OrgHierarchy;
import org.sunbird.org.model.OrgHierarchyInfo;
import org.sunbird.org.service.ExtendedOrgServiceImpl;
import org.sunbird.orghierarchyreport.entity.MdoChildrenLookupEntity;
import org.sunbird.orghierarchyreport.repository.MdoChildrenLookUpRepository;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class OrgLevelHierarchyServiceImpl implements OrgLevelHierarchyService {

    private final Logger logger = LoggerFactory.getLogger(OrgLevelHierarchyServiceImpl.class);

    private final MdoChildrenLookUpRepository mdoChildrenLookupRepository;

    private final CbExtServerProperties configProperties;

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    private final ExtendedOrgServiceImpl extendedOrgService;

    private final RedisCacheMgr redisCacheMgr;

    private final ObjectMapper objectMapper;

    public OrgLevelHierarchyServiceImpl(MdoChildrenLookUpRepository mdoChildrenLookupRepository,
                                        CbExtServerProperties configProperties, OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
                                        ExtendedOrgServiceImpl extendedOrgService, RedisCacheMgr redisCacheMgr, ObjectMapper objectMapper) {
        this.mdoChildrenLookupRepository = mdoChildrenLookupRepository;
        this.configProperties = configProperties;
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.extendedOrgService = extendedOrgService;
        this.redisCacheMgr = redisCacheMgr;
        this.objectMapper = objectMapper;
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
        List<Map<String, Object>> cachedChildrenDataList = new ArrayList<>();
        List<String> missingChildrenIds = new ArrayList<>();
        for (String identifier : indentifiersList) {
            String cacheKey = "orghierarchy:report:" + identifier;
            String cachedChildrenData = redisCacheMgr.getCache(cacheKey);
            if (StringUtils.isNotEmpty(cachedChildrenData)) {
                try {
                    List<Map<String, Object>> cachedDataList = objectMapper.readValue(cachedChildrenData, new TypeReference<List<Map<String, Object>>>() {
                    });
                    cachedChildrenDataList.addAll(cachedDataList);
                } catch (IOException e) {
                    logger.error("Error while parsing cached children data for org id: " + identifier, e);
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrmsg("Error while parsing cached children data");
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                    return response;
                }
            } else {
                missingChildrenIds.add(identifier);
            }
        }
        if (CollectionUtils.isNotEmpty(missingChildrenIds)) {
            indentifiersList = new ArrayList<>(missingChildrenIds);
            List<MdoChildrenLookupEntity> childrenIdsList = mdoChildrenLookupRepository.findAllChildrenByMdoId(indentifiersList);
            if (CollectionUtils.isEmpty(childrenIdsList)) {
                Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
                Map<String, Object> filters = (Map<String, Object>) requestBody.get(Constants.FILTERS);
                filters.put(Constants.IDENTIFIER, indentifiersList);
                return handleNoChildrenScenario(request, cachedChildrenDataList);
            }
            handleChildrenScenario(childrenIdsList, response, cachedChildrenDataList);
        } else {
            response.put(Constants.COUNT, cachedChildrenDataList.size());
            response.put(Constants.RESPONSE, cachedChildrenDataList);
        }
        return response;
    }

    private void handleChildrenScenario(List<MdoChildrenLookupEntity> childrenIdsList, SBApiResponse response, List<Map<String, Object>> cachedChildrenDataList) {
        List<Map<String, Object>> mappedList = new ArrayList<>();
        for (MdoChildrenLookupEntity entity : childrenIdsList) {
            String childrenIds = entity.getChildrenId();
            List<String> orgIds = Arrays.asList(childrenIds.split(","));
            Map<String, Object> requestMap = new HashMap<>();
            requestMap.put(Constants.OFFSET, configProperties.getOrgLevelHierarchyESOffset());
            requestMap.put(Constants.LIMIT, configProperties.getOrgLevelHierarchyESLimit());
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
            mappedList.addAll(buildMappedOrgList(responseMap));
            cachedChildrenDataList.addAll(mappedList);
            redisCacheMgr.putCache(Constants.ORG_LEVEL_HIERARCHY_CACHE_KEY + entity.getMdoId(), mappedList, configProperties.getOrgLevelHierarchyCacheKeyTTL());
        }
        Map<String, Object> contentMap = new HashMap<>();
        contentMap.put(Constants.COUNT, cachedChildrenDataList.size());
        contentMap.put(Constants.CONTENT, cachedChildrenDataList);
        response.put(Constants.RESPONSE, contentMap);
    }

    private SBApiResponse handleNoChildrenScenario(Map<String, Object> request, List<Map<String, Object>> cachedChildrenDataList) {
        Map<String, Object> requestBody = (Map<String, Object>) request.get(Constants.REQUEST);
        Map<String, Object> filters = (Map<String, Object>) requestBody.get(Constants.FILTERS);
        List<String> indentifiersList = (List<String>) filters.get(Constants.IDENTIFIER);
        SBApiResponse listAllOrgResponse = new SBApiResponse();
        for (String identifier : indentifiersList) {
            filters.put(Constants.IDENTIFIER, Collections.singletonList(identifier));
            SBApiResponse orgSearchResponse = extendedOrgService.orgExtSearchV2(request);
            Map<String, Object> result = orgSearchResponse.getResult();
            if (MapUtils.isNotEmpty(result)) {
                List<OrgHierarchyInfo> responseList = (List<OrgHierarchyInfo>) result.get(Constants.RESPONSE);
                if (CollectionUtils.isNotEmpty(responseList)) {
                    listAllOrgResponse = extendedOrgService.listAllOrg(responseList.get(0).getMapId());
                    result = listAllOrgResponse.getResult();
                    if (MapUtils.isNotEmpty(result)) {
                        Map<String, Object> responseMap = (Map<String, Object>) result.get(Constants.RESPONSE);
                        List<OrgHierarchy> contentList = (List<OrgHierarchy>) responseMap.get(Constants.CONTENT);
                        if (CollectionUtils.isNotEmpty(contentList)) {
                            List<Map<String, Object>> modifiedContentList = contentList.stream()
                                    .map(org -> (Map<String, Object>) objectMapper.convertValue(org, new TypeReference<Map<String, Object>>() {
                                    }))
                                    .collect(Collectors.toList());
                            cachedChildrenDataList.addAll(modifiedContentList);
                            redisCacheMgr.putCache(Constants.ORG_LEVEL_HIERARCHY_CACHE_KEY + identifier, modifiedContentList, configProperties.getOrgLevelHierarchyCacheKeyTTL());
                        }
                    }
                }
            }
        }
        Map<String, Object> contentMap = new HashMap<>();
        contentMap.put(Constants.CONTENT, cachedChildrenDataList);
        contentMap.put(Constants.COUNT, cachedChildrenDataList.size());
        listAllOrgResponse.put(Constants.RESPONSE, contentMap);
        return listAllOrgResponse;
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
