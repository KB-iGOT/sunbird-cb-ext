package org.sunbird.migrate.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrgHierarchyServiceClientTest {

    private static final String ORG_ID = "org-root-1";
    private static final String SERVICE_URL = "http://localhost:8888";
    private static final String COMPLETE_PATH = "/org/v2/hierarchy";
    private static final String ACCESS_TOKEN = "test-access-token";
    private static final int CACHE_TTL_SECONDS = 14400;
    private static final String CACHE_KEY = Constants.ORG_HIERARCHY_SERVICE_MASTER_DATA_CACHE_KEY_PREFIX + ORG_ID;

    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;
    private RedisCacheMgr redisCacheMgr;
    private OrgHierarchyServiceClient client;

    @BeforeEach
    void setUp() {
        outboundRequestHandlerService = mock(OutboundRequestHandlerServiceImpl.class);
        redisCacheMgr = mock(RedisCacheMgr.class);
        CbExtServerProperties serverConfig = mock(CbExtServerProperties.class);
        when(serverConfig.getOrgHierarchyServiceUrl()).thenReturn(SERVICE_URL);
        when(serverConfig.getOrgHierarchyServiceCompletePath()).thenReturn(COMPLETE_PATH);
        when(serverConfig.getOrgHierarchyServiceMasterDataRedisTtl()).thenReturn(CACHE_TTL_SECONDS);
        client = new OrgHierarchyServiceClient(outboundRequestHandlerService, serverConfig, redisCacheMgr, new ObjectMapper());
    }

    private Map<String, Object> orgNode(String id, String name, List<Map<String, Object>> associations) {
        Map<String, Object> node = new HashMap<>();
        node.put(Constants.ID, id);
        node.put(Constants.NAME, name);
        if (associations != null) {
            node.put(Constants.ASSOCIATIONS, associations);
        }
        return node;
    }

    private Map<String, Object> apiResponse(Map<String, Object> result) {
        Map<String, Object> response = new HashMap<>();
        response.put(Constants.RESPONSE_CODE, Constants.OK);
        response.put(Constants.RESULT, result);
        return response;
    }

    @Test
    void fetchMasterData_cacheHit_returnsCachedDataWithoutCallingApi() throws Exception {
        when(redisCacheMgr.getCache(CACHE_KEY)).thenReturn("[{\"id\":\"org-root-1\",\"channel\":\"Root Org\"}]");

        List<Map<String, String>> result = client.fetchMasterData(ORG_ID, ACCESS_TOKEN);

        assertEquals(1, result.size());
        assertEquals("org-root-1", result.get(0).get(Constants.ID));
        assertEquals("Root Org", result.get(0).get(Constants.CHANNEL));
        verify(outboundRequestHandlerService, never()).fetchUsingGetWithHeaders(anyString(), anyMap());
        verify(redisCacheMgr, never()).putCache(anyString(), any(), anyInt());
    }

    @Test
    void fetchMasterData_cacheMiss_flattensNestedTreeAndCachesResult() throws Exception {
        when(redisCacheMgr.getCache(CACHE_KEY)).thenReturn(null);
        Map<String, Object> child = orgNode("org-child-1", "Child Org", null);
        Map<String, Object> root = orgNode(ORG_ID, "Root Org", Collections.singletonList(child));
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenReturn(apiResponse(root));

        List<Map<String, String>> result = client.fetchMasterData(ORG_ID, ACCESS_TOKEN);

        assertEquals(2, result.size());
        assertEquals(ORG_ID, result.get(0).get(Constants.ID));
        assertEquals("org-child-1", result.get(1).get(Constants.ID));
        assertEquals("Child Org", result.get(1).get(Constants.CHANNEL));
        verify(redisCacheMgr, times(1)).putCache(eq(CACHE_KEY), eq(result), eq(CACHE_TTL_SECONDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    void fetchMasterData_requestsCompleteEndpointWithLevelRequiredFalseParamAndAuthHeader() throws Exception {
        when(redisCacheMgr.getCache(CACHE_KEY)).thenReturn(null);
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenReturn(apiResponse(orgNode(ORG_ID, "Root Org", null)));

        client.fetchMasterData(ORG_ID, ACCESS_TOKEN);

        ArgumentCaptor<String> urlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
        verify(outboundRequestHandlerService).fetchUsingGetWithHeaders(urlCaptor.capture(), headersCaptor.capture());

        assertEquals(SERVICE_URL + COMPLETE_PATH + "/" + ORG_ID + "/" + Constants.COMPLETE
                + "?" + Constants.LEVEL_REQUIRED + "=false", urlCaptor.getValue());
        assertEquals(ACCESS_TOKEN, headersCaptor.getValue().get(Constants.X_AUTH_TOKEN));
    }

    @Test
    void fetchMasterData_nonOkResponseCode_returnsEmptyListAndDoesNotCache() throws Exception {
        when(redisCacheMgr.getCache(CACHE_KEY)).thenReturn(null);
        Map<String, Object> failedResponse = new HashMap<>();
        failedResponse.put(Constants.RESPONSE_CODE, "FAILED");
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenReturn(failedResponse);

        List<Map<String, String>> result = client.fetchMasterData(ORG_ID, ACCESS_TOKEN);

        assertTrue(result.isEmpty());
        verify(redisCacheMgr, never()).putCache(anyString(), any(), anyInt());
    }

    @Test
    void fetchMasterData_emptyApiResponse_returnsEmptyListAndDoesNotCache() throws Exception {
        when(redisCacheMgr.getCache(CACHE_KEY)).thenReturn(null);
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenReturn(null);

        List<Map<String, String>> result = client.fetchMasterData(ORG_ID, ACCESS_TOKEN);

        assertTrue(result.isEmpty());
        verify(redisCacheMgr, never()).putCache(anyString(), any(), anyInt());
    }

    @Test
    void fetchMasterData_resultNodeWithoutId_producesEmptyListAndDoesNotCache() throws Exception {
        when(redisCacheMgr.getCache(CACHE_KEY)).thenReturn(null);
        Map<String, Object> nodeWithoutId = new HashMap<>();
        nodeWithoutId.put(Constants.NAME, "No Id Org");
        when(outboundRequestHandlerService.fetchUsingGetWithHeaders(anyString(), anyMap()))
                .thenReturn(apiResponse(nodeWithoutId));

        List<Map<String, String>> result = client.fetchMasterData(ORG_ID, ACCESS_TOKEN);

        assertTrue(result.isEmpty());
        verify(redisCacheMgr, never()).putCache(anyString(), any(), anyInt());
    }
}
