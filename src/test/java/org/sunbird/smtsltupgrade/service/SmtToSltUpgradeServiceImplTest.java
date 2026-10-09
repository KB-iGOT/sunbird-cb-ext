package org.sunbird.smtsltupgrade.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.mockito.ArgumentCaptor;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpServer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.model.SearchUserAPIResponse;
import org.sunbird.common.model.SearchUserApiContent;
import org.sunbird.common.model.SearchUserApiResp;
import org.sunbird.common.model.SearchUserApiRespResult;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorRoleEntity;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRepository;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRoleRepository;
import org.sunbird.user.service.UserUtilityService;

import com.fasterxml.jackson.databind.ObjectMapper;

class SmtToSltUpgradeServiceImplTest {

    private static final String SMT_USER_ID = "eb05d807-8122-4d25-987f-6aad17a99107";
    private static final short SMT_ROLE_ID = 30;
    private static final short SLT_ROLE_ID = 20;

    @Mock
    private CbExtServerProperties props;

    @Mock
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ProgramCoordinatorRepository programCoordinatorRepository;

    @Mock
    private ProgramCoordinatorRoleRepository programCoordinatorRoleRepository;

    @Mock
    private UserUtilityService userUtilityService;

    private TestableSmtToSltUpgradeService service;

    /**
     * Overrides the ES seam method so tests can stub search results/exceptions without
     * mocking RestHighLevelClient's final search() method.
     */
    private static class TestableSmtToSltUpgradeService extends SmtToSltUpgradeServiceImpl {
        private final Deque<Object> searchResults = new ArrayDeque<>();

        TestableSmtToSltUpgradeService(CbExtServerProperties props,
                OutboundRequestHandlerServiceImpl outboundRequestHandlerService, ObjectMapper objectMapper,
                ProgramCoordinatorRepository programCoordinatorRepository,
                ProgramCoordinatorRoleRepository programCoordinatorRoleRepository,
                UserUtilityService userUtilityService) {
            super(props, outboundRequestHandlerService, objectMapper, null, programCoordinatorRepository,
                    programCoordinatorRoleRepository, userUtilityService);
        }

        void enqueueSearchResult(Object searchResponseOrException) {
            searchResults.add(searchResponseOrException);
        }

        @Override
        protected SearchResponse executeSearch(SearchRequest searchRequest) throws java.io.IOException {
            Object next = searchResults.poll();
            if (next instanceof RuntimeException) {
                throw (RuntimeException) next;
            }
            return (SearchResponse) next;
        }
    }

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        service = new TestableSmtToSltUpgradeService(props, outboundRequestHandlerService, objectMapper,
                programCoordinatorRepository, programCoordinatorRoleRepository, userUtilityService);
        ReflectionTestUtils.setField(service, "smtRoleCode", "STATE_MASTER_TRAINER");
        ReflectionTestUtils.setField(service, "sltRoleCode", "STATE_LEAD_TRAINER");
        ReflectionTestUtils.setField(service, "minCompletedBatches", 3);
        ReflectionTestUtils.setField(service, "minLearnersTrained", 100);

        when(props.getSbUrl()).thenReturn("http://learner-service:9000");
        when(props.getUserSearchEndPoint()).thenReturn("private/user/v1/search");
        when(props.getLmsUserUpdatePrivatePath()).thenReturn("/private/user/v1/update");

        when(programCoordinatorRoleRepository.findByRoleCode("STATE_MASTER_TRAINER"))
                .thenReturn(java.util.Optional.of(
                        ProgramCoordinatorRoleEntity.builder().id(SMT_ROLE_ID).roleCode("STATE_MASTER_TRAINER").build()));
        when(programCoordinatorRoleRepository.findByRoleCode("STATE_LEAD_TRAINER"))
                .thenReturn(java.util.Optional.of(
                        ProgramCoordinatorRoleEntity.builder().id(SLT_ROLE_ID).roleCode("STATE_LEAD_TRAINER").build()));
    }

    private SearchUserApiResp buildSmtSearchResponse(String... userIds) {
        SearchUserApiResp resp = new SearchUserApiResp();
        resp.setResponseCode(Constants.OK);
        SearchUserApiRespResult result = new SearchUserApiRespResult();
        SearchUserAPIResponse response = new SearchUserAPIResponse();
        List<SearchUserApiContent> contents = new ArrayList<>();
        for (String userId : userIds) {
            SearchUserApiContent content = new SearchUserApiContent();
            content.setUserId(userId);
            contents.add(content);
        }
        response.setContent(contents);
        response.setCount(contents.size());
        result.setResponse(response);
        resp.setResult(result);
        return resp;
    }

    private SearchResponse buildSearchResponse(List<Map<String, Object>> batchSources) {
        SearchResponse searchResponse = mock(SearchResponse.class);
        SearchHit[] hits = new SearchHit[batchSources.size()];
        ObjectMapper realObjectMapper = new ObjectMapper();
        for (int i = 0; i < batchSources.size(); i++) {
            try {
                SearchHit hit = new SearchHit(i);
                hit.sourceRef(new BytesArray(realObjectMapper.writeValueAsBytes(batchSources.get(i))));
                hits[i] = hit;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        SearchHits searchHits = new SearchHits(hits, hits.length, 1.0f);
        when(searchResponse.getHits()).thenReturn(searchHits);
        return searchResponse;
    }

    private Map<String, Object> batchSource(String batchId, int enrolmentCount) {
        Map<String, Object> batch = new HashMap<>();
        batch.put(Constants.FIELD_BATCH_ID, batchId);
        batch.put(Constants.FIELD_ENROLMENT_COUNT, enrolmentCount);
        return batch;
    }

    @Test
    void shouldReturnOkWithZeroScannedWhenNoSmtUsersFound() {
        Map<String, Object> rawResponse = new HashMap<>();
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(0, response.get(Constants.SCANNED_COUNT));
        assertEquals(0, response.get(Constants.UPGRADED_COUNT));
        assertEquals(0, response.get(Constants.FAILED_COUNT));
        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), any(), anyMap());
    }

    @Test
    void shouldNotUpgradeWhenThresholdsNotMet() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(buildSearchResponse(Collections.singletonList(batchSource("batch-1", 10))));

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, response.get(Constants.SCANNED_COUNT));
        assertEquals(0, response.get(Constants.UPGRADED_COUNT));
        verify(outboundRequestHandlerService, never()).fetchResultUsingPatch(anyString(), any(), anyMap());
    }

    @Test
    void shouldUpgradeUserWhenCompletedBatchThresholdMet() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10),
                batchSource("batch-2", 10),
                batchSource("batch-3", 10))));

        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, response.get(Constants.SCANNED_COUNT));
        assertEquals(1, response.get(Constants.UPGRADED_COUNT));
        assertEquals(0, response.get(Constants.FAILED_COUNT));
        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), any(), anyMap());

        verify(programCoordinatorRepository).updateRoleIdForUser(
                java.util.UUID.fromString(SMT_USER_ID), SMT_ROLE_ID, SLT_ROLE_ID);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldPreserveExistingProfileDetailsWhenUpgrading() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10),
                batchSource("batch-2", 10),
                batchSource("batch-3", 10))));

        Map<String, Object> existingProfileDetails = new HashMap<>();
        existingProfileDetails.put("personalDetails", Collections.singletonMap("firstname", "Trainer One"));
        existingProfileDetails.put(Constants.BP_CO_TRAINER, "STATE_MASTER_TRAINER");
        Map<String, Object> existingUserData = new HashMap<>();
        existingUserData.put(Constants.PROFILE_DETAILS, existingProfileDetails);
        when(userUtilityService.getUsersReadData(SMT_USER_ID, "", "")).thenReturn(existingUserData);

        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);

        service.runSmtToSltUpgradeCheck();

        ArgumentCaptor<Map<String, Object>> requestCaptor = ArgumentCaptor.forClass(Map.class);
        verify(outboundRequestHandlerService).fetchResultUsingPatch(anyString(), requestCaptor.capture(), anyMap());

        Map<String, Object> sentRequestBody =
                (Map<String, Object>) requestCaptor.getValue().get(Constants.REQUEST);
        Map<String, Object> sentProfileDetails =
                (Map<String, Object>) sentRequestBody.get(Constants.PROFILE_DETAILS);
        assertEquals("STATE_LEAD_TRAINER", sentProfileDetails.get(Constants.BP_CO_TRAINER));
        assertEquals(Collections.singletonMap("firstname", "Trainer One"),
                sentProfileDetails.get("personalDetails"));
    }

    @Test
    void shouldStartFromEmptyProfileDetailsWhenExistingReadReturnsNothing() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10),
                batchSource("batch-2", 10),
                batchSource("batch-3", 10))));

        when(userUtilityService.getUsersReadData(SMT_USER_ID, "", "")).thenReturn(Collections.emptyMap());

        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(1, response.get(Constants.UPGRADED_COUNT));
    }

    @Test
    void shouldNotSyncProgramCoordinatorRoleWhenProfileUpdateFails() {
        stubSmtSearch(buildSmtSearchResponse(SMT_USER_ID));
        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10), batchSource("batch-2", 10), batchSource("batch-3", 10))));
        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, "CLIENT_ERROR");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);

        service.runSmtToSltUpgradeCheck();

        verify(programCoordinatorRepository, never()).updateRoleIdForUser(any(), any(), any());
    }

    @Test
    void shouldStillUpgradeWhenRoleIdResolutionFailsForSync() {
        stubSmtSearch(buildSmtSearchResponse(SMT_USER_ID));
        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10), batchSource("batch-2", 10), batchSource("batch-3", 10))));
        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);
        when(programCoordinatorRoleRepository.findByRoleCode("STATE_MASTER_TRAINER"))
                .thenReturn(java.util.Optional.empty());

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(1, response.get(Constants.UPGRADED_COUNT));
        verify(programCoordinatorRepository, never()).updateRoleIdForUser(any(), any(), any());
    }

    @Test
    void shouldStillUpgradeWhenSyncThrowsException() {
        stubSmtSearch(buildSmtSearchResponse(SMT_USER_ID));
        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10), batchSource("batch-2", 10), batchSource("batch-3", 10))));
        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);
        when(programCoordinatorRepository.updateRoleIdForUser(any(), any(), any()))
                .thenThrow(new RuntimeException("db down"));

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(1, response.get(Constants.UPGRADED_COUNT));
    }

    @Test
    void shouldUpgradeUserWhenLearnersTrainedThresholdMet() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(buildSearchResponse(Collections.singletonList(batchSource("batch-1", 150))));

        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, Constants.OK);
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(1, response.get(Constants.UPGRADED_COUNT));
    }

    @Test
    void shouldCountFailureWhenUpgradeApiCallFails() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10),
                batchSource("batch-2", 10),
                batchSource("batch-3", 10))));

        Map<String, Object> patchResponse = new HashMap<>();
        patchResponse.put(Constants.RESPONSE_CODE, "CLIENT_ERROR");
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(patchResponse);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(0, response.get(Constants.UPGRADED_COUNT));
        assertEquals(1, response.get(Constants.FAILED_COUNT));
    }

    @Test
    void shouldReturnInternalServerErrorWhenSmtLookupThrows() {
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenThrow(new RuntimeException("search service down"));

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals("search service down", response.get(Constants.ERROR_MESSAGE));
    }

    @Test
    void shouldCountFailureWhenBatchLookupThrowsForOneUser() {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class))
                .thenReturn(buildSmtSearchResponse(SMT_USER_ID));

        service.enqueueSearchResult(new RuntimeException("es unavailable"));

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(1, response.get(Constants.SCANNED_COUNT));
        assertEquals(1, response.get(Constants.FAILED_COUNT));
    }

    private void stubSmtSearch(SearchUserApiResp searchResp) {
        Map<String, Object> rawResponse = new HashMap<>();
        rawResponse.put("dummy", "value");
        when(outboundRequestHandlerService.fetchResultUsingPost(anyString(), anyMap(), eq(null)))
                .thenReturn(rawResponse);
        when(objectMapper.convertValue(rawResponse, SearchUserApiResp.class)).thenReturn(searchResp);
    }

    @Test
    void shouldCountFailureWhenUpgradeApiCallThrows() {
        stubSmtSearch(buildSmtSearchResponse(SMT_USER_ID));
        service.enqueueSearchResult(buildSearchResponse(Arrays.asList(
                batchSource("batch-1", 10), batchSource("batch-2", 10), batchSource("batch-3", 10))));
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenThrow(new RuntimeException("update api down"));

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(0, response.get(Constants.UPGRADED_COUNT));
        assertEquals(1, response.get(Constants.FAILED_COUNT));
    }

    @Test
    void shouldCountFailureWhenUpgradeApiReturnsEmptyResponse() {
        stubSmtSearch(buildSmtSearchResponse(SMT_USER_ID));
        service.enqueueSearchResult(buildSearchResponse(Collections.singletonList(batchSource("batch-1", 150))));
        when(outboundRequestHandlerService.fetchResultUsingPatch(anyString(), any(), anyMap()))
                .thenReturn(new HashMap<>());

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(1, response.get(Constants.FAILED_COUNT));
    }

    @Test
    void shouldReturnZeroScannedWhenSearchResponseNotOk() {
        SearchUserApiResp resp = buildSmtSearchResponse(SMT_USER_ID);
        resp.setResponseCode("CLIENT_ERROR");
        stubSmtSearch(resp);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(0, response.get(Constants.SCANNED_COUNT));
    }

    @Test
    void shouldReturnZeroScannedWhenSearchResponseIsNull() {
        stubSmtSearch(null);

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(0, response.get(Constants.SCANNED_COUNT));
    }

    @Test
    void shouldReturnZeroScannedWhenSearchResultMissing() {
        SearchUserApiResp resp = new SearchUserApiResp();
        resp.setResponseCode(Constants.OK);
        stubSmtSearch(resp);

        assertEquals(0, service.runSmtToSltUpgradeCheck().get(Constants.SCANNED_COUNT));
    }

    @Test
    void shouldReturnZeroScannedWhenSearchResponseBodyMissing() {
        SearchUserApiResp resp = new SearchUserApiResp();
        resp.setResponseCode(Constants.OK);
        resp.setResult(new SearchUserApiRespResult());
        stubSmtSearch(resp);

        assertEquals(0, service.runSmtToSltUpgradeCheck().get(Constants.SCANNED_COUNT));
    }

    @Test
    void shouldReturnZeroScannedWhenSearchContentEmpty() {
        stubSmtSearch(buildSmtSearchResponse());

        assertEquals(0, service.runSmtToSltUpgradeCheck().get(Constants.SCANNED_COUNT));
    }

    @Test
    void shouldSkipSmtUsersWithBlankUserId() {
        stubSmtSearch(buildSmtSearchResponse(" "));

        assertEquals(0, service.runSmtToSltUpgradeCheck().get(Constants.SCANNED_COUNT));
    }

    @Test
    void shouldIgnoreBatchesWithoutIdAndNonNumericEnrolmentCount() {
        stubSmtSearch(buildSmtSearchResponse(SMT_USER_ID));
        Map<String, Object> noId = new HashMap<>();
        noId.put(Constants.FIELD_ENROLMENT_COUNT, "many");
        service.enqueueSearchResult(buildSearchResponse(Collections.singletonList(noId)));

        SBApiResponse response = service.runSmtToSltUpgradeCheck();

        assertEquals(1, response.get(Constants.SCANNED_COUNT));
        assertEquals(0, response.get(Constants.UPGRADED_COUNT));
        assertEquals(0, response.get(Constants.FAILED_COUNT));
    }

    @Test
    void shouldDelegateSearchToEsClient() throws Exception {
        String body = "{\"took\":1,\"timed_out\":false,\"_shards\":{\"total\":1,\"successful\":1,\"skipped\":0,"
                + "\"failed\":0},\"hits\":{\"total\":0,\"max_score\":null,\"hits\":[]}}";
        HttpServer esStub = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        esStub.createContext("/", exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        esStub.start();
        try (RestHighLevelClient esClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", esStub.getAddress().getPort())))) {
            SmtToSltUpgradeServiceImpl realService = new SmtToSltUpgradeServiceImpl(props,
                    outboundRequestHandlerService, objectMapper, esClient, programCoordinatorRepository,
                    programCoordinatorRoleRepository, userUtilityService);

            SearchResponse response = realService.executeSearch(new SearchRequest("batch"));

            assertEquals(0, response.getHits().getHits().length);
        } finally {
            esStub.stop(0);
        }
    }
}
