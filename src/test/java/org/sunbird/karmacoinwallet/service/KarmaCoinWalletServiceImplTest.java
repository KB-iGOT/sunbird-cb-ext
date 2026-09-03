package org.sunbird.karmacoinwallet.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.YearMonth;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.consumer.KafkaProducer;

public class KarmaCoinWalletServiceImplTest {

    private static final String TOKEN = "valid-token";
    private static final String USER_ID = "user-123";
    private static final String AUTHORIZED_ROLE = "PUBLIC";
    private static final int MONTHLY_CAP = 100;
    private static final int REDIS_TTL = 3600;

    @Mock
    private CassandraOperation cassandraOperation;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private CbExtServerProperties serverProperties;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private KafkaProducer kafkaProducer;

    @InjectMocks
    private KarmaCoinWalletServiceImpl service;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    private void mockAuthenticatedAndAuthorized() {
        Map<String, Object> payload = new HashMap<>();
        when(accessTokenValidator.extractTokenPayload(TOKEN)).thenReturn(payload);
        when(accessTokenValidator.getUserIdFromPayload(payload)).thenReturn(USER_ID);
        when(accessTokenValidator.getUserRolesFromPayload(payload))
                .thenReturn(Collections.singletonList(AUTHORIZED_ROLE));
        when(serverProperties.getKarmaCoinWalletAuthorizedRoles())
                .thenReturn(Collections.singletonList(AUTHORIZED_ROLE));
    }

    private void mockBlankUser() {
        Map<String, Object> payload = new HashMap<>();
        when(accessTokenValidator.extractTokenPayload(TOKEN)).thenReturn(payload);
        when(accessTokenValidator.getUserIdFromPayload(payload)).thenReturn(StringEmpty());
    }

    private void mockUnauthorizedRole() {
        Map<String, Object> payload = new HashMap<>();
        when(accessTokenValidator.extractTokenPayload(TOKEN)).thenReturn(payload);
        when(accessTokenValidator.getUserIdFromPayload(payload)).thenReturn(USER_ID);
        when(accessTokenValidator.getUserRolesFromPayload(payload))
                .thenReturn(Collections.singletonList("SOME_OTHER_ROLE"));
        when(serverProperties.getKarmaCoinWalletAuthorizedRoles())
                .thenReturn(Collections.singletonList(AUTHORIZED_ROLE));
    }

    private static String StringEmpty() {
        return "";
    }

    /**
     * Stubs the wallet / monthly-summary / karma-points Cassandra reads used by the summary and
     * redeem flows. Redis cache is treated as a miss so the values come from Cassandra.
     */
    private void mockCassandraWallet(int totalEarned, int totalRedeemed, int convertedThisMonth,
            int totalKarmaPoints) {
        when(redisCacheMgr.getCache(Constants.REDIS_KEY_KARMA_COINS + USER_ID)).thenReturn(null);

        Map<String, Object> walletRow = new HashMap<>();
        walletRow.put(Constants.TOTAL_EARNED, totalEarned);
        walletRow.put(Constants.TOTAL_REDEEMED, totalRedeemed);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_COIN_WALLET), anyMap(), anyList()))
                .thenReturn(Collections.singletonList(walletRow));

        Map<String, Object> monthlyRow = new HashMap<>();
        monthlyRow.put(Constants.POINTS_CONVERTED, convertedThisMonth);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_COIN_MONTHLY_SUMMARY), anyMap(), anyList()))
                .thenReturn(Collections.singletonList(monthlyRow));

        Map<String, Object> pointsRow = new HashMap<>();
        pointsRow.put(Constants.TOTAL_POINTS, totalKarmaPoints);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_POINTS_SUMMARY), anyMap(), anyList()))
                .thenReturn(Collections.singletonList(pointsRow));

        when(serverProperties.getKarmaCoinMonthlyCap()).thenReturn(MONTHLY_CAP);
        when(serverProperties.getKarmaCoinWalletRedisTtl()).thenReturn(REDIS_TTL);
    }

    // ------------------------------------------------------------------
    // getWalletSummary
    // ------------------------------------------------------------------

    @Test
    public void getWalletSummary_blankUser_returnsUnauthorized() {
        mockBlankUser();

        SBApiResponse response = service.getWalletSummary(TOKEN);

        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.USER_ID_DOESNT_EXIST, response.getParams().getErrmsg());
    }

    @Test
    public void getWalletSummary_unauthorizedRole_returnsForbidden() {
        mockUnauthorizedRole();

        SBApiResponse response = service.getWalletSummary(TOKEN);

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        assertEquals(Constants.UNAUTHORIZED_USER, response.getParams().getErrmsg());
    }

    @Test
    public void getWalletSummary_success_computesWalletFigures() {
        mockAuthenticatedAndAuthorized();
        // totalEarned=40, totalRedeemed=10, converted=20, totalKarmaPoints=100
        mockCassandraWallet(40, 10, 20, 100);

        SBApiResponse response = service.getWalletSummary(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.SUCCESS, response.getParams().getStatus());
        Map<String, Object> result = response.getResult();
        assertEquals(30, result.get(Constants.WALLET_BALANCE));          // 40 - 10
        assertEquals(10, result.get(Constants.TOTAL_REDEEMED_CAMEL));
        assertEquals(40, result.get(Constants.TOTAL_EARNED_TILL_DATE));
        assertEquals(100, result.get(Constants.TOTAL_KARMA_POINTS));
        assertEquals(60, result.get(Constants.UNREDEEMED_KARMA_POINTS));  // 100 - 40
        assertEquals(MONTHLY_CAP, result.get(Constants.MONTHLY_CAP));
        assertEquals(20, result.get(Constants.CONVERTED_THIS_MONTH));
        // min(cap - converted=80, unredeemed=60) = 60
        assertEquals(60, result.get(Constants.CONVERTIBLE_THIS_MONTH));
        assertEquals(Boolean.TRUE, result.get(Constants.REDEEM_ENABLED));
        assertEquals(YearMonth.now().toString(), result.get(Constants.YEAR_MONTH_CAMEL));
        assertNotNull(result.get(Constants.CAP_RESETS_ON));
        // cache miss => value re-cached
        verify(redisCacheMgr).putStringInCache(eq(Constants.REDIS_KEY_KARMA_COINS + USER_ID),
                anyString(), eq(REDIS_TTL));
    }

    @Test
    public void getWalletSummary_capReached_redeemDisabled() {
        mockAuthenticatedAndAuthorized();
        // everything already earned & cap consumed => nothing convertible
        mockCassandraWallet(100, 0, MONTHLY_CAP, 100);

        SBApiResponse response = service.getWalletSummary(TOKEN);

        Map<String, Object> result = response.getResult();
        assertEquals(0, result.get(Constants.CONVERTIBLE_THIS_MONTH));
        assertEquals(Boolean.FALSE, result.get(Constants.REDEEM_ENABLED));
    }

    @Test
    public void getWalletSummary_usesCacheWhenYearMonthMatches() {
        mockAuthenticatedAndAuthorized();
        String cached = "{\"totalEarned\":40,\"totalRedeemed\":10,\"convertedThisMonth\":20,\"yearMonth\":\""
                + YearMonth.now().toString() + "\"}";
        when(redisCacheMgr.getCache(Constants.REDIS_KEY_KARMA_COINS + USER_ID)).thenReturn(cached);
        when(serverProperties.getKarmaCoinMonthlyCap()).thenReturn(MONTHLY_CAP);
        // karma points summary is always read from Cassandra (not cached)
        Map<String, Object> pointsRow = new HashMap<>();
        pointsRow.put(Constants.TOTAL_POINTS, 100);
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_POINTS_SUMMARY), anyMap(), anyList()))
                .thenReturn(Collections.singletonList(pointsRow));

        SBApiResponse response = service.getWalletSummary(TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(30, response.getResult().get(Constants.WALLET_BALANCE));
        // wallet & monthly tables must NOT be hit when the cache is warm
        verify(cassandraOperation, never()).getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_COIN_WALLET), anyMap(), anyList());
        verify(redisCacheMgr, never()).putStringInCache(anyString(), anyString(), anyInt());
    }

    @Test
    public void getWalletSummary_cassandraThrows_returnsInternalServerError() {
        mockAuthenticatedAndAuthorized();
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(cassandraOperation.getRecordsByProperties(anyString(), anyString(), anyMap(), anyList()))
                .thenThrow(new RuntimeException("cassandra down"));

        SBApiResponse response = service.getWalletSummary(TOKEN);

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
    }

    // ------------------------------------------------------------------
    // getTransactions
    // ------------------------------------------------------------------

    private Map<String, Object> transactionRequest(String startDate, String endDate, String type) {
        Map<String, Object> request = new HashMap<>();
        request.put(Constants.START_DATE, startDate);
        request.put(Constants.END_DATE, endDate);
        if (type != null) {
            request.put(Constants.TYPE, type);
        }
        Map<String, Object> body = new HashMap<>();
        body.put(Constants.REQUEST, request);
        return body;
    }

    @Test
    public void getTransactions_blankUser_returnsUnauthorized() {
        mockBlankUser();

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", null));

        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
    }

    @Test
    public void getTransactions_unauthorizedRole_returnsForbidden() {
        mockUnauthorizedRole();

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", null));

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    public void getTransactions_missingRequestObject_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getTransactions(TOKEN, new HashMap<>());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_REQUEST, response.getParams().getErrmsg());
    }

    @Test
    public void getTransactions_nullBody_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getTransactions(TOKEN, null);

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    public void getTransactions_endBeforeStart_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-31", "2026-01-01", null));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_REQUEST, response.getParams().getErrmsg());
    }

    @Test
    public void getTransactions_unparseableDate_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("not-a-date", "2026-01-01", null));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    public void getTransactions_invalidType_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getTransactions(TOKEN,
                transactionRequest("2026-01-01", "2026-01-31", "TRANSFER"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_REQUEST, response.getParams().getErrmsg());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_typeAll_returnsAllRows() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_COIN_TRANSACTIONS), anyMap(), anyList(),
                eq(Constants.DB_COLUMN_TXN_CREATED_AT), any(), any()))
                .thenReturn(Arrays.asList(row(Constants.TXN_TYPE_CREDIT), row(Constants.TXN_TYPE_DEBIT)));

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(2, txns.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_typeCredit_filtersRows() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Arrays.asList(row(Constants.TXN_TYPE_CREDIT), row(Constants.TXN_TYPE_DEBIT)));

        SBApiResponse response = service.getTransactions(TOKEN,
                transactionRequest("2026-01-01", "2026-01-31", "credit"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(1, txns.size());
        assertEquals(Constants.TXN_TYPE_CREDIT, txns.get(0).get(Constants.TYPE));
    }

    @Test
    public void getTransactions_cassandraThrows_returnsInternalServerError() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenThrow(new RuntimeException("cassandra down"));

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }

    private Map<String, Object> row(String type) {
        Map<String, Object> row = new HashMap<>();
        row.put(Constants.DB_COLUMN_TRANSACTION_ID, "txn-" + type);
        row.put(Constants.DB_COLUMN_TXN_CREATED_AT, 123L);
        row.put(Constants.TYPE, type);
        row.put(Constants.DB_COLUMN_AMOUNT, 10);
        return row;
    }

    private Map<String, Object> redeemRequest(Object points, Object requestId) {
        Map<String, Object> request = new HashMap<>();
        if (points != null) {
            request.put(Constants.POINTS_TO_CONVERT, points);
        }
        if (requestId != null) {
            request.put(Constants.REQUEST_ID, requestId);
        }
        Map<String, Object> body = new HashMap<>();
        body.put(Constants.REQUEST, request);
        return body;
    }

    @Test
    public void redeem_blankUser_returnsUnauthorized() {
        mockBlankUser();

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(10, "req-1"));

        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
    }

    @Test
    public void redeem_unauthorizedRole_returnsForbidden() {
        mockUnauthorizedRole();

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(10, "req-1"));

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    public void redeem_missingRequestObject_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.redeem(TOKEN, new HashMap<>());

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    public void redeem_nonNumericPoints_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.redeem(TOKEN, redeemRequest("ten", "req-1"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    public void redeem_nonPositivePoints_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(0, "req-1"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    public void redeem_blankRequestId_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(10, "   "));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
    }

    @Test
    public void redeem_exceedsConvertibleCap_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();
        // convertible = min(cap-converted=80, unredeemed=60) = 60; asking for 61
        mockCassandraWallet(40, 10, 20, 100);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(61, "req-1"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.MONTHLY_CAP_EXCEEDED, response.getParams().getErrmsg());
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void redeem_success_pushesEventAndReturnsAccepted() {
        mockAuthenticatedAndAuthorized();
        mockCassandraWallet(40, 10, 20, 100);
        when(serverProperties.getKarmaCoinWalletRedeemTopic()).thenReturn("karma-redeem-topic");
        // claim applied => first time seeing this request
        when(cassandraOperation.insertRecordIfNotExists(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap())).thenReturn(true);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(50, "req-1"));

        assertEquals(HttpStatus.ACCEPTED, response.getResponseCode());
        assertEquals(Constants.ACCEPTED, response.getParams().getStatus());
        // the PROCESSING claim row must be written before publishing
        verify(cassandraOperation).insertRecordIfNotExists(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap());
        Map<String, Object> result = response.getResult();
        assertEquals("req-1", result.get(Constants.REQUEST_ID));
        assertEquals(Constants.PROCESSING, result.get(Constants.STATUS));

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq("karma-redeem-topic"), eq(USER_ID), eventCaptor.capture());

        // Envelope: { eventType, data: { ... } }
        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals(Constants.POINTS_CONVERSION, event.get(Constants.EVENT_TYPE));
        Map<String, Object> data = (Map<String, Object>) event.get(Constants.DATA);
        assertNotNull(data);
        assertEquals(Constants.KARMA_COIN_CREDIT_EID, data.get(Constants.EVENT_EID));
        assertEquals(USER_ID, data.get(Constants.USER_ID));
        assertEquals("req-1", data.get(Constants.REQUEST_ID));
        assertEquals(Constants.TXN_TYPE_CREDIT, data.get(Constants.OPERATION));
        assertEquals(Constants.POINTS_CONVERSION, data.get(Constants.ACTION_TYPE_CAMEL));
        assertEquals(50, data.get(Constants.POINTS_TO_CONVERT));
        assertEquals(Constants.POINTS_CONVERSION, data.get(Constants.CONTEXT_TYPE));
        assertEquals("req-1", data.get(Constants.CONTEXT_ID_CAMEL));
        assertEquals(YearMonth.now().toString(), data.get(Constants.CONVERSION_PERIOD));
        assertNotNull(data.get(Constants.EVENT_ETS));
    }

    @Test
    public void redeem_kafkaThrows_marksFailedAndReturnsInternalServerError() {
        mockAuthenticatedAndAuthorized();
        mockCassandraWallet(40, 10, 20, 100);
        when(serverProperties.getKarmaCoinWalletRedeemTopic()).thenReturn("karma-redeem-topic");
        when(cassandraOperation.insertRecordIfNotExists(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap())).thenReturn(true);
        org.mockito.Mockito.doThrow(new RuntimeException("kafka down"))
                .when(kafkaProducer).push(anyString(), anyString(), any());

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(50, "req-1"));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
        // claim was applied, so the row is overwritten to a FAILED terminal state (plain insert)
        verify(cassandraOperation).insertRecord(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap());
    }

    @Test
    public void redeem_duplicateRequestId_returnsExistingStatusWithoutPublishing() {
        mockAuthenticatedAndAuthorized();
        mockCassandraWallet(40, 10, 20, 100);
        // claim NOT applied => duplicate requestId already seen
        when(cassandraOperation.insertRecordIfNotExists(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap())).thenReturn(false);
        // existing lookup row already SUCCESS
        Map<String, Object> lookupRow = new HashMap<>();
        lookupRow.put(Constants.ADDINFO,
                "{\"status\":\"SUCCESS\",\"transactionId\":\"TXN-1\",\"pointsConverted\":50}");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap(), any()))
                .thenReturn(Collections.singletonList(lookupRow));

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(50, "req-1"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        Map<String, Object> result = response.getResult();
        assertEquals("req-1", result.get(Constants.REQUEST_ID));
        assertEquals("SUCCESS", result.get(Constants.STATUS));
        // must NOT republish the event on a duplicate
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    // ------------------------------------------------------------------
    // getRedeemStatus
    // ------------------------------------------------------------------

    @Test
    public void getRedeemStatus_blankUser_returnsUnauthorized() {
        mockBlankUser();

        SBApiResponse response = service.getRedeemStatus(TOKEN, "req-1");

        assertEquals(HttpStatus.UNAUTHORIZED, response.getResponseCode());
    }

    @Test
    public void getRedeemStatus_unauthorizedRole_returnsForbidden() {
        mockUnauthorizedRole();

        SBApiResponse response = service.getRedeemStatus(TOKEN, "req-1");

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
    }

    @Test
    public void getRedeemStatus_blankRequestId_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getRedeemStatus(TOKEN, "   ");

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INVALID_REQUEST, response.getParams().getErrmsg());
    }

    @Test
    public void getRedeemStatus_noRecord_returnsNotFound() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap(), any()))
                .thenReturn(Collections.emptyList());

        SBApiResponse response = service.getRedeemStatus(TOKEN, "req-1");

        assertEquals(HttpStatus.NOT_FOUND, response.getResponseCode());
        assertEquals(Constants.REDEEM_REQUEST_NOT_FOUND, response.getParams().getErrmsg());
    }

    @Test
    public void getRedeemStatus_recordExists_returnsStatusFromAddInfo() {
        mockAuthenticatedAndAuthorized();
        Map<String, Object> lookupRow = new HashMap<>();
        lookupRow.put(Constants.ADDINFO,
                "{\"status\":\"SUCCESS\",\"transactionId\":\"TXN-9\",\"pointsConverted\":30}");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap(), any()))
                .thenReturn(Collections.singletonList(lookupRow));

        SBApiResponse response = service.getRedeemStatus(TOKEN, "req-1");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        Map<String, Object> result = response.getResult();
        assertEquals("req-1", result.get(Constants.REQUEST_ID));
        assertEquals("SUCCESS", result.get(Constants.STATUS));
        assertEquals("TXN-9", result.get("transactionId"));
        assertEquals(30, result.get("pointsConverted"));
    }

    @Test
    public void getRedeemStatus_blankAddInfo_returnsProcessing() {
        mockAuthenticatedAndAuthorized();
        Map<String, Object> lookupRow = new HashMap<>();
        lookupRow.put(Constants.ADDINFO, "");
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap(), any()))
                .thenReturn(Collections.singletonList(lookupRow));

        SBApiResponse response = service.getRedeemStatus(TOKEN, "req-1");

        assertEquals(HttpStatus.OK, response.getResponseCode());
        assertEquals(Constants.PROCESSING, response.getResult().get(Constants.STATUS));
    }

    @Test
    public void getRedeemStatus_cassandraThrows_returnsInternalServerError() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByProperties(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.USER_KARMA_COIN_LOOKUP), anyMap(), any()))
                .thenThrow(new RuntimeException("cassandra down"));

        SBApiResponse response = service.getRedeemStatus(TOKEN, "req-1");

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
    }
}
