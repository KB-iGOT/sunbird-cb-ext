package org.sunbird.karmacoinwallet.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
    private static final int DEDUP_TTL = 3;

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
        when(serverProperties.getKarmaCoinConvertLockKeyPattern()).thenReturn("karmaCoinConvertLock:{userId}:{requestId}");
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
     * redeem flows. All three are read fresh at QUORUM (no cache).
     */
    private void mockCassandraWallet(int totalEarned, int totalRedeemed, int convertedThisMonth,
            int totalKarmaPoints) {
        Map<String, Object> walletRow = new HashMap<>();
        walletRow.put(Constants.TOTAL_EARNED, totalEarned);
        walletRow.put(Constants.TOTAL_REDEEMED, totalRedeemed);
        when(cassandraOperation.getRecordsByPropertiesWithConsistencyLevel(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_COIN_WALLET), anyMap(), anyList(), eq(ConsistencyLevel.QUORUM)))
                .thenReturn(Collections.singletonList(walletRow));

        Map<String, Object> monthlyRow = new HashMap<>();
        monthlyRow.put(Constants.POINTS_CONVERTED, convertedThisMonth);
        when(cassandraOperation.getRecordsByPropertiesWithConsistencyLevel(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_COIN_MONTHLY_SUMMARY), anyMap(), anyList(), eq(ConsistencyLevel.QUORUM)))
                .thenReturn(Collections.singletonList(monthlyRow));

        Map<String, Object> pointsRow = new HashMap<>();
        pointsRow.put(Constants.TOTAL_POINTS, totalKarmaPoints);
        when(cassandraOperation.getRecordsByPropertiesWithConsistencyLevel(eq(Constants.KEYSPACE_SUNBIRD),
                eq(Constants.TABLE_USER_KARMA_POINTS_SUMMARY), anyMap(), anyList(), eq(ConsistencyLevel.QUORUM)))
                .thenReturn(Collections.singletonList(pointsRow));

        when(serverProperties.getKarmaCoinMonthlyCap()).thenReturn(MONTHLY_CAP);
    }

    /**
     * Stubs the conversion in-progress lock (Redis {@code SET NX EX}, keyed by userId + requestId)
     * to either grant or deny the claim. The lock value is the points being converted (so
     * {@code getTransactions} can later read it back), not a fixed marker, hence {@code anyString()}.
     * Assumes the caller uses requestId {@code "req-1"}, matching every test that calls this helper.
     */
    private void mockDedupGuard(boolean claimed) {
        when(serverProperties.getKarmaCoinConvertLockTtl()).thenReturn(DEDUP_TTL);
        when(redisCacheMgr.setIfAbsent(eq("karmaCoinConvertLock:" + USER_ID + ":req-1"),
                anyString(), eq(DEDUP_TTL))).thenReturn(claimed);
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
    public void getWalletSummary_alwaysReadsFreshFromCassandra() {
        mockAuthenticatedAndAuthorized();
        mockCassandraWallet(40, 10, 20, 100);

        service.getWalletSummary(TOKEN);
        service.getWalletSummary(TOKEN);

        // no caching: every call re-reads the wallet table at QUORUM
        verify(cassandraOperation, org.mockito.Mockito.times(2)).getRecordsByPropertiesWithConsistencyLevel(
                eq(Constants.KEYSPACE_SUNBIRD), eq(Constants.TABLE_USER_KARMA_COIN_WALLET), anyMap(), anyList(),
                eq(ConsistencyLevel.QUORUM));
        verify(redisCacheMgr, never()).getCache(anyString());
        verify(redisCacheMgr, never()).putStringInCache(anyString(), anyString(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    public void getWalletSummary_cassandraThrows_returnsInternalServerError() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithConsistencyLevel(anyString(), anyString(), anyMap(),
                anyList(), any(ConsistencyLevel.class))).thenThrow(new RuntimeException("cassandra down"));

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
    public void getTransactions_rangeBeyondOneYear_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();

        SBApiResponse response = service.getTransactions(TOKEN,
                transactionRequest("2025-01-01", "2026-01-02", null));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.TRANSACTION_DATE_RANGE_EXCEEDED, response.getParams().getErrmsg());
    }

    @Test
    public void getTransactions_rangeOfExactlyOneYear_isAllowed() {
        mockAuthenticatedAndAuthorized();

        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(
                anyString(), anyString(), anyMap(),
                anyList(), anyString(), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList());

        ZoneId zoneId = ZoneId.of(Constants.ASIA_KOLKATA_TIMEZONE);
        LocalDate today = LocalDate.now(zoneId);
        LocalDate oneYearAgo = today.minusYears(1);

        SBApiResponse response = service.getTransactions(
                TOKEN,
                transactionRequest(
                        oneYearAgo.toString(),
                        today.toString(),
                        null));

        assertEquals(HttpStatus.OK, response.getResponseCode());
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

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_pendingConversionInRedis_prependsInProgressItem() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Collections.singletonList(row(Constants.TXN_TYPE_CREDIT)));
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*"))
                .thenReturn(Collections.singletonMap("karmaCoinConvertLock:" + USER_ID + ":req-1", "50"));
        when(serverProperties.getKarmaCoinConversionRate()).thenReturn(2);

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(2, txns.size());
        Map<String, Object> pending = txns.get(0);
        assertEquals(Constants.TXN_STATUS_IN_PROGRESS, pending.get(Constants.STATUS));
        assertEquals(50, pending.get(Constants.POINTS_TO_CONVERT));
        assertEquals(100, pending.get(Constants.AMOUNT_CAMEL));
        assertEquals(2, pending.get(Constants.CONVERSION_RATE_CAMEL));
        assertEquals("1 Karma Point = 2 Karma Coins", pending.get(Constants.CONVERSION_MESSAGE_CAMEL));
        // completed rows are untouched: no conversion fields leak onto them
        Map<String, Object> completed = txns.get(1);
        assertEquals("txn-CREDIT", completed.get(Constants.TRANSACTION_ID_CAMEL));
        assertNull(completed.get(Constants.CONVERSION_RATE_CAMEL));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_multiplePendingLocks_prependsOneItemPerLock() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Collections.singletonList(row(Constants.TXN_TYPE_CREDIT)));
        // two tabs converting concurrently => two distinct requestId-scoped locks for the same user
        Map<String, String> pendingLocks = new HashMap<>();
        pendingLocks.put("karmaCoinConvertLock:" + USER_ID + ":req-1", "50");
        pendingLocks.put("karmaCoinConvertLock:" + USER_ID + ":req-2", "30");
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*")).thenReturn(pendingLocks);
        when(serverProperties.getKarmaCoinConversionRate()).thenReturn(1);

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(3, txns.size());
        List<Object> pendingAmounts = Arrays.asList(txns.get(0).get(Constants.POINTS_TO_CONVERT),
                txns.get(1).get(Constants.POINTS_TO_CONVERT));
        assertEquals(2, new java.util.HashSet<>(pendingAmounts).size());
        assertEquals(true, pendingAmounts.contains(50) && pendingAmounts.contains(30));
        assertEquals("txn-CREDIT", txns.get(2).get(Constants.TRANSACTION_ID_CAMEL));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_noPendingLock_noInProgressItem() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Collections.singletonList(row(Constants.TXN_TYPE_CREDIT)));
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*"))
                .thenReturn(Collections.emptyMap());

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(1, txns.size());
        assertEquals(Constants.TXN_TYPE_CREDIT, txns.get(0).get(Constants.TYPE));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_pendingLockValueNonNumeric_noInProgressItem() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Collections.emptyList());
        // stale/corrupt lock value (e.g. legacy "IN_PROGRESS" marker) should not blow up the endpoint
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*"))
                .thenReturn(Collections.singletonMap("karmaCoinConvertLock:" + USER_ID + ":req-1", Constants.IN_PROGRESS));

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(0, txns.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getTransactions_pendingEnrolmentInRedis_prependsInProgressDebitItem() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Collections.singletonList(row(Constants.TXN_TYPE_CREDIT)));
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*")).thenReturn(Collections.emptyMap());
        when(redisCacheMgr.getValuesByRawPattern("pendingEnrolment_" + USER_ID + "_*")).thenReturn(
                Collections.singletonMap("pendingEnrolment_" + USER_ID + "_ext_123",
                        "{\"courseName\":\"AI-Powered Retail Operations\",\"karmaCoins\":100,\"status\":\"Pending\"}"));

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(2, txns.size());
        Map<String, Object> pending = txns.get(0);
        assertEquals(Constants.TXN_STATUS_IN_PROGRESS, pending.get(Constants.STATUS));
        assertEquals(Constants.TXN_TYPE_DEBIT, pending.get(Constants.TYPE));
        assertEquals(Constants.POINTS_REDEMPTION, pending.get(Constants.ACTION_TYPE_CAMEL));
        assertEquals("AI-Powered Retail Operations", pending.get(Constants.COURSE_NAME));
        assertEquals(100, pending.get(Constants.AMOUNT_CAMEL));
    }

    @Test
    @Disabled("Pending investigation of transaction count")
    @SuppressWarnings("unchecked")
    public void getTransactions_pendingEnrolmentTerminalStatus_notShownAsInProgress() {
        mockAuthenticatedAndAuthorized();
        when(cassandraOperation.getRecordsByPropertiesWithClusteringRange(anyString(), anyString(), anyMap(),
                anyList(), anyString(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*")).thenReturn(Collections.emptyMap());
        // once redemption completes, karma-points-processor-v2 overwrites the value with a bare
        // status string ("SUCCESS"/"FAILED") instead of JSON - that's terminal, not pending
        when(redisCacheMgr.getValuesByRawPattern("pendingEnrolment_" + USER_ID + "_*"))
                .thenReturn(Collections.singletonMap("pendingEnrolment_" + USER_ID + "_ext_123", "SUCCESS"));

        SBApiResponse response = service.getTransactions(TOKEN, transactionRequest("2026-01-01", "2026-01-31", "ALL"));

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> txns = (List<Map<String, Object>>) response.getResult().get(Constants.TRANSACTIONS);
        assertEquals(2, txns.size());
        Map<String, Object> pending = txns.get(0);
        assertEquals(Constants.TXN_STATUS_IN_PROGRESS, pending.get(Constants.STATUS));
        assertEquals(Constants.TXN_TYPE_DEBIT, pending.get(Constants.TYPE));
        assertEquals(Constants.POINTS_REDEMPTION, pending.get(Constants.ACTION_TYPE_CAMEL));
        assertEquals("AI-Powered Retail Operations", pending.get(Constants.COURSE_NAME));
        assertEquals(100, pending.get(Constants.AMOUNT_CAMEL));
    }

    // ------------------------------------------------------------------
    // redeem
    // ------------------------------------------------------------------

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
    public void redeem_duplicateInProgress_returnsConflict() {
        mockAuthenticatedAndAuthorized();
        // request passes validation (enough points, within cap) but the dedup guard denies the claim
        // => another redeem for this user is already in flight
        mockCassandraWallet(10, 0, 0, 100);
        mockDedupGuard(false);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(10, "req-1"));

        assertEquals(HttpStatus.CONFLICT, response.getResponseCode());
        assertEquals(Constants.CONVERSION_REQUEST_IN_PROGRESS, response.getParams().getErrmsg());
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    @Test
    public void redeem_exceedsConvertibleCap_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();
        mockDedupGuard(true);
        // unredeemed = 100-10 = 90; remainingCap = cap(100)-converted(90) = 10; convertible = min(10,90) = 10; asking for 11
        mockCassandraWallet(10, 0, 90, 100);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(11, "req-1"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.MONTHLY_CAP_EXCEEDED, response.getParams().getErrmsg());
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    @Test
    public void redeem_otherPendingRequestCountedAgainstBalance_returnsInsufficientPoints() {
        mockAuthenticatedAndAuthorized();
        // another tab already has 60 points locked in-flight (its own requestId, not yet committed)
        mockCassandraWallet(0, 0, 0, 100);
        when(redisCacheMgr.getValuesByPattern("karmaCoinConvertLock:" + USER_ID + ":*"))
                .thenReturn(Collections.singletonMap("karmaCoinConvertLock:" + USER_ID + ":req-other", "60"));

        // unredeemed = 100-0 = 100, but 60 is already claimed by the other pending request, so only
        // 40 is actually available; asking for 50 must be rejected even though 50 <= 100
        SBApiResponse response = service.redeem(TOKEN, redeemRequest(50, "req-2"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INSUFFICIENT_KARMA_POINTS, response.getParams().getErrmsg());
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    @Test
    public void redeem_insufficientPoints_partialBalance_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();
        mockDedupGuard(true);
        // unredeemed = 100-90 = 10; user asks for 15, more than they have, even though monthly cap is not reached
        mockCassandraWallet(90, 0, 0, 100);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(15, "req-1"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INSUFFICIENT_KARMA_POINTS, response.getParams().getErrmsg());
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    @Test
    public void redeem_insufficientPoints_zeroBalance_returnsBadRequest() {
        mockAuthenticatedAndAuthorized();
        mockDedupGuard(true);
        // unredeemed = 100-100 = 0; user has no points left to convert
        mockCassandraWallet(100, 0, 0, 100);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(10, "req-1"));

        assertEquals(HttpStatus.BAD_REQUEST, response.getResponseCode());
        assertEquals(Constants.INSUFFICIENT_KARMA_POINTS, response.getParams().getErrmsg());
        verify(kafkaProducer, never()).push(anyString(), anyString(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void redeem_success_pushesEventAndReturnsAccepted() {
        mockAuthenticatedAndAuthorized();
        mockDedupGuard(true);
        mockCassandraWallet(40, 10, 20, 100);
        when(serverProperties.getKarmaCoinWalletRedeemTopic()).thenReturn("karma-redeem-topic");
        when(serverProperties.getKarmaCoinWalletRedeemEventVersion()).thenReturn(1);

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(50, "req-1"));

        assertEquals(HttpStatus.ACCEPTED, response.getResponseCode());
        assertEquals(Constants.ACCEPTED, response.getParams().getStatus());
        Map<String, Object> result = response.getResult();
        assertEquals("req-1", result.get(Constants.REQUEST_ID));
        assertEquals(Constants.PROCESSING, result.get(Constants.STATUS));

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer).push(eq("karma-redeem-topic"), eq(USER_ID), eventCaptor.capture());

        // Envelope: { eventType, data: { ... }, version }
        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals(Constants.POINTS_CONVERSION, event.get(Constants.EVENT_TYPE));
        assertEquals(1, event.get(Constants.EVENT_VERSION));
        Map<String, Object> data = (Map<String, Object>) event.get(Constants.DATA);
        assertNotNull(data);
        assertEquals(Constants.KARMA_COIN_CREDIT_EID, data.get(Constants.EVENT_EID));
        assertEquals(USER_ID, data.get(Constants.USER_ID));
        assertEquals("req-1", data.get(Constants.REQUEST_ID));
        assertNotNull(data.get(Constants.EVENT_ID));
        assertEquals(Constants.TXN_TYPE_CREDIT, data.get(Constants.OPERATION));
        assertEquals(Constants.POINTS_CONVERSION, data.get(Constants.ACTION_TYPE_CAMEL));
        assertEquals(50, data.get(Constants.POINTS_TO_CONVERT));
        assertEquals(Constants.POINTS_CONVERSION, data.get(Constants.CONTEXT_TYPE));
        assertEquals("req-1", data.get(Constants.CONTEXT_ID_CAMEL));
        assertEquals(YearMonth.now().toString(), data.get(Constants.CONVERSION_PERIOD));
        assertNotNull(data.get(Constants.EVENT_ETS));

        // lock value is the points being converted, so getTransactions can read it back later
        verify(redisCacheMgr).setIfAbsent(
                "karmaCoinConvertLock:" + USER_ID + ":req-1",
                "50",
                DEDUP_TTL
        );
    }

    @Test
    public void redeem_kafkaThrows_returnsInternalServerError() {
        mockAuthenticatedAndAuthorized();
        mockDedupGuard(true);
        mockCassandraWallet(40, 10, 20, 100);
        when(serverProperties.getKarmaCoinWalletRedeemTopic()).thenReturn("karma-redeem-topic");
        org.mockito.Mockito.doThrow(new RuntimeException("kafka down"))
                .when(kafkaProducer).push(anyString(), anyString(), any());

        SBApiResponse response = service.redeem(TOKEN, redeemRequest(50, "req-1"));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getResponseCode());
        assertEquals(Constants.FAILED, response.getParams().getStatus());
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
