package org.sunbird.migrate.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.migrate.exception.OrgHierarchyServiceException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class OrgHierarchyServiceClient {

    private final Logger log = LoggerFactory.getLogger(getClass().getName());

    private final OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private final CbExtServerProperties serverConfig;
    private final RedisCacheMgr redisCacheMgr;
    private final ObjectMapper mapper;

    public OrgHierarchyServiceClient(OutboundRequestHandlerServiceImpl outboundRequestHandlerService,
            CbExtServerProperties serverConfig, RedisCacheMgr redisCacheMgr, ObjectMapper mapper) {
        this.outboundRequestHandlerService = outboundRequestHandlerService;
        this.serverConfig = serverConfig;
        this.redisCacheMgr = redisCacheMgr;
        this.mapper = mapper;
    }

    public List<Map<String, String>> fetchMasterData(String orgId, String accessToken) {
        String cacheKey = Constants.ORG_HIERARCHY_SERVICE_MASTER_DATA_CACHE_KEY_PREFIX + orgId;
        Optional<List<Map<String, String>>> cachedMasterData = readFromCache(cacheKey, orgId);
        if (cachedMasterData.isPresent()) {
            return cachedMasterData.get();
        }

        Map<String, Object> response = callCompleteApi(orgId, accessToken);
        if (MapUtils.isEmpty(response) || !Constants.OK.equals(response.get(Constants.RESPONSE_CODE))) {
            log.error("cb-org-hierarchy-service /complete call failed for orgId: {}. Response: {}", orgId, response);
            return Collections.emptyList();
        }

        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESULT);
        List<Map<String, String>> masterData = new ArrayList<>();
        flattenOrgTree(result, masterData);

        if (CollectionUtils.isNotEmpty(masterData)) {
            redisCacheMgr.putCache(cacheKey, masterData, serverConfig.getOrgHierarchyServiceMasterDataRedisTtl());
        }
        return masterData;
    }

    private Optional<List<Map<String, String>>> readFromCache(String cacheKey, String orgId) {
        String cachedDataStr = redisCacheMgr.getCache(cacheKey);
        if (StringUtils.isEmpty(cachedDataStr) ||
                Constants.NULL_STRING.equalsIgnoreCase(cachedDataStr) ||
                Constants.EMPTY_JSON_ARRAY.equals(cachedDataStr)) {
            return Optional.empty();
        }
        log.info("Cache hit for fetchMasterData, orgId: {}", orgId);
        try {
            return Optional.of(mapper.readValue(cachedDataStr, new TypeReference<List<Map<String, String>>>() {
            }));
        } catch (IOException e) {
            throw new OrgHierarchyServiceException("Failed to parse cached org hierarchy master data for orgId: " + orgId, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> callCompleteApi(String orgId, String accessToken) {
        String url = UriComponentsBuilder
                .fromHttpUrl(serverConfig.getOrgHierarchyServiceUrl() + serverConfig.getOrgHierarchyServiceCompletePath()
                        + "/" + orgId + "/" + Constants.COMPLETE)
                .queryParam(Constants.LEVEL_REQUIRED, Constants.FALSE)
                .toUriString();
        Map<String, String> headers = new HashMap<>();
        headers.put(Constants.X_AUTH_TOKEN, accessToken);
        return (Map<String, Object>) outboundRequestHandlerService.fetchUsingGetWithHeaders(url, headers);
    }

    /** Depth-first walk of {@code /complete}'s {@code associations} tree, collecting one {id, name} entry per node. */
    @SuppressWarnings("unchecked")
    private void flattenOrgTree(Map<String, Object> node, List<Map<String, String>> collector) {
        if (MapUtils.isEmpty(node)) {
            return;
        }
        String id = (String) node.get(Constants.ID);
        if (StringUtils.isNotEmpty(id)) {
            Map<String, String> entry = new HashMap<>();
            entry.put(Constants.ID, id);
            entry.put(Constants.CHANNEL, (String) node.get(Constants.NAME));
            collector.add(entry);
        }

        List<Map<String, Object>> children = (List<Map<String, Object>>) node.get(Constants.ASSOCIATIONS);
        if (CollectionUtils.isNotEmpty(children)) {
            for (Map<String, Object> child : children) {
                flattenOrgTree(child, collector);
            }
        }
    }
}
