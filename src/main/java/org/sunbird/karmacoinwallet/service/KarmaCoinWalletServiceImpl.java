package org.sunbird.karmacoinwallet.service;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.sunbird.consumer.KafkaProducer;

@Service
public class KarmaCoinWalletServiceImpl implements KarmaCoinWalletService {

    private final CassandraOperation cassandraOperation;

    private final RedisCacheMgr redisCacheMgr;

    private final CbExtServerProperties serverProperties;

    private final AccessTokenValidator accessTokenValidator;

    private final KafkaProducer kafkaProducer;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    public KarmaCoinWalletServiceImpl(CassandraOperation cassandraOperation, RedisCacheMgr redisCacheMgr,
                                      CbExtServerProperties serverProperties, AccessTokenValidator accessTokenValidator, KafkaProducer kafkaProducer) {
        this.cassandraOperation = cassandraOperation;
        this.redisCacheMgr = redisCacheMgr;
        this.serverProperties = serverProperties;
        this.accessTokenValidator = accessTokenValidator;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public SBApiResponse getWalletSummary(String token) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_KARMA_WALLET_SUMMARY);
        Map<String, Object> tokenPayload = accessTokenValidator.extractTokenPayload(token);
        String userId = accessTokenValidator.getUserIdFromPayload(tokenPayload);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.UNAUTHORIZED);
            return response;
        }

        List<String> userRoles = accessTokenValidator.getUserRolesFromPayload(tokenPayload);
        List<String> authorizedRoles = serverProperties.getKarmaCoinWalletAuthorizedRoles();
        if (CollectionUtils.isEmpty(userRoles) || userRoles.stream().noneMatch(authorizedRoles::contains)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(Constants.UNAUTHORIZED_USER);
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return response;
        }
        try {
            String currentYearMonth = YearMonth.now().toString();

            WalletSnapshot snapshot = loadWalletAndMonthly(userId, currentYearMonth);
            int totalEarned = snapshot.totalEarned;
            int totalRedeemed = snapshot.totalRedeemed;
            int convertedThisMonth = snapshot.convertedThisMonth;

            int totalKarmaPoints = fetchTotalKarmaPoints(userId);

            int monthlyCap = serverProperties.getKarmaCoinMonthlyCap();
            if (totalRedeemed > totalEarned || totalEarned > totalKarmaPoints) {
                logger.warn(
                        "Karma coin summary drift for user {}: totalEarned={}, totalRedeemed={}, totalKarmaPoints={}",
                        userId, totalEarned, totalRedeemed, totalKarmaPoints);
            }
            int walletBalance = Math.max(0, totalEarned - totalRedeemed);
            int unredeemedKarmaPoints = Math.max(0, totalKarmaPoints - totalEarned);
            int convertibleThisMonth = Math.max(0, Math.min(monthlyCap - convertedThisMonth, unredeemedKarmaPoints));
            boolean redeemEnabled = convertibleThisMonth > 0;
            String capResetsOn = YearMonth.now().plusMonths(1).atDay(1).toString();

            Map<String, Object> result = new HashMap<>();
            result.put(Constants.WALLET_BALANCE, walletBalance);
            result.put(Constants.TOTAL_REDEEMED_CAMEL, totalRedeemed);
            result.put(Constants.TOTAL_EARNED_TILL_DATE, totalEarned);
            result.put(Constants.TOTAL_KARMA_POINTS, totalKarmaPoints);
            result.put(Constants.UNREDEEMED_KARMA_POINTS, unredeemedKarmaPoints);
            result.put(Constants.YEAR_MONTH_CAMEL, currentYearMonth);
            result.put(Constants.MONTHLY_CAP, monthlyCap);
            result.put(Constants.CONVERTED_THIS_MONTH, convertedThisMonth);
            result.put(Constants.CONVERTIBLE_THIS_MONTH, convertibleThisMonth);
            result.put(Constants.CAP_RESETS_ON, capResetsOn);
            result.put(Constants.REDEEM_ENABLED, redeemEnabled);

            response.setResult(result);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Failed to fetch karma coin wallet summary for user: " + userId, e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg("Failed to fetch karma coin wallet summary");
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse getTransactions(String token, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_KARMA_WALLET_TRANSACTIONS);
        Map<String, Object> tokenPayload = accessTokenValidator.extractTokenPayload(token);
        String userId = accessTokenValidator.getUserIdFromPayload(tokenPayload);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
            response.setResponseCode(HttpStatus.UNAUTHORIZED);
            return response;
        }

        List<String> userRoles = accessTokenValidator.getUserRolesFromPayload(tokenPayload);
        List<String> authorizedRoles = serverProperties.getKarmaCoinWalletAuthorizedRoles();
        if (CollectionUtils.isEmpty(userRoles) || userRoles.stream().noneMatch(authorizedRoles::contains)) {
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(Constants.UNAUTHORIZED_USER);
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return response;
        }


        Object requestObj = (requestBody == null) ? null : requestBody.get(Constants.REQUEST);
        if (!(requestObj instanceof Map)) {
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<?, ?> request = (Map<?, ?>) requestObj;

        long startDate;
        long endDate;
        try {
            LocalDate fromDate = LocalDate.parse(String.valueOf(request.get(Constants.START_DATE)));
            LocalDate toDate = LocalDate.parse(String.valueOf(request.get(Constants.END_DATE)));
            if (toDate.isBefore(fromDate)) {
                setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
                return response;
            }
            ZoneId zoneId = ZoneId.of(Constants.ASIA_KOLKATA_TIMEZONE);
            startDate = fromDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
            endDate = toDate.plusDays(1).atStartOfDay(zoneId).toInstant().toEpochMilli() - 1;
        } catch (Exception e) {
            logger.debug("Invalid date range in karma coin transactions request", e);
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }

        String type = resolveType(request.get(Constants.TYPE));
        if (type == null) {
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }

        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.KARMA_POINTS_USER_ID, userId);
            List<Map<String, Object>> rows = cassandraOperation.getRecordsByPropertiesWithClusteringRange(
                    Constants.KEYSPACE_SUNBIRD, Constants.TABLE_USER_KARMA_COIN_TRANSACTIONS, propertyMap,
                    new ArrayList<>(), Constants.DB_COLUMN_TXN_CREATED_AT, startDate, endDate);

            List<Map<String, Object>> transactions = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                String rowType = (String) row.get(Constants.TYPE);
                if (!Constants.TXN_TYPE_ALL.equals(type) && !type.equals(rowType)) {
                    continue;
                }
                transactions.add(toTransactionView(row));
            }

            Map<String, Object> result = new HashMap<>();
            result.put(Constants.TRANSACTIONS, transactions);
            response.setResult(result);
            response.getParams().setStatus(Constants.SUCCESS);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Failed to fetch karma coin wallet transactions for user: " + userId, e);
            setError(response, "Failed to fetch karma coin wallet transactions", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    /**
     * Normalises the request {@code type} to one of {@code ALL} (default), {@code CREDIT} or
     * {@code DEBIT}. Returns {@code null} for any unrecognised value so the caller can reject it.
     */
    private String resolveType(Object typeValue) {
        String type = (typeValue == null || StringUtils.isBlank(typeValue.toString()))
                ? Constants.TXN_TYPE_ALL
                : typeValue.toString().toUpperCase(Locale.ENGLISH);
        if (Constants.TXN_TYPE_ALL.equals(type) || Constants.TXN_TYPE_CREDIT.equals(type)
                || Constants.TXN_TYPE_DEBIT.equals(type)) {
            return type;
        }
        return null;
    }

    private void setError(SBApiResponse response, String errMsg, HttpStatus status) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(status);
    }

    /**
     * Maps a raw {@code user_karma_coin_transactions} row (keyed by DB column names) to the
     * camelCase view the UI consumes. {@code addinfo} is passed through as the stored JSON string.
     */
    private Map<String, Object> toTransactionView(Map<String, Object> row) {
        Map<String, Object> txn = new HashMap<>();
        txn.put(Constants.TRANSACTION_ID_CAMEL, row.get(Constants.DB_COLUMN_TRANSACTION_ID));
        txn.put(Constants.DATE_CAMEL, row.get(Constants.DB_COLUMN_TXN_CREATED_AT));
        txn.put(Constants.TYPE, row.get(Constants.TYPE));
        txn.put(Constants.AMOUNT_CAMEL, row.get(Constants.DB_COLUMN_AMOUNT));
        txn.put(Constants.BALANCE_AFTER_CAMEL, row.get(Constants.DB_COLUMN_BALANCE_AFTER));
        txn.put(Constants.ACTION_TYPE_CAMEL, row.get(Constants.DB_COLUMN_ACTION_TYPE));
        txn.put(Constants.CONTEXT_TYPE_CAMEL, row.get(Constants.DB_CLOUMN_CONTEXT_TYPE));
        txn.put(Constants.CONTEXT_ID_CAMEL, row.get(Constants.DB_COLUMN_CONTEXT_ID));
        txn.put(Constants.ADDINFO_CAMEL, row.get(Constants.DB_COLUMN_ADDINFO));
        return txn;
    }

    /**
     * Loads {@code totalEarned}, {@code totalRedeemed} and {@code convertedThisMonth} for the
     * user. The cached JSON is scoped to a year-month, so a month rollover is treated as a miss
     * and the values are re-read from Cassandra and re-cached.
     */
    private WalletSnapshot loadWalletAndMonthly(String userId, String currentYearMonth) {
        String cacheKey = Constants.REDIS_KEY_KARMA_COINS + userId;
        try {
            String cached = redisCacheMgr.getCache(cacheKey);
            if (StringUtils.isNotBlank(cached)) {
                Map<String, Object> cachedMap = objectMapper.readValue(cached, Map.class);
                if (currentYearMonth.equals(cachedMap.get(Constants.YEAR_MONTH_CAMEL))) {
                    return new WalletSnapshot(
                            toInt(cachedMap.get(Constants.TOTAL_EARNED_CAMEL)),
                            toInt(cachedMap.get(Constants.TOTAL_REDEEMED_CAMEL)),
                            toInt(cachedMap.get(Constants.CONVERTED_THIS_MONTH)));
                }
            }
        } catch (Exception e) {
            logger.error("Failed to read karma coin wallet cache for user: " + userId, e);
        }

        int totalEarned = 0;
        int totalRedeemed = 0;
        Map<String, Object> walletProps = new HashMap<>();
        walletProps.put(Constants.KARMA_POINTS_USER_ID, userId);
        List<Map<String, Object>> walletRows = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER_KARMA_COIN_WALLET, walletProps, new ArrayList<>());
        if (CollectionUtils.isNotEmpty(walletRows)) {
            Map<String, Object> wallet = walletRows.get(0);
            totalEarned = toInt(wallet.get(Constants.TOTAL_EARNED));
            totalRedeemed = toInt(wallet.get(Constants.TOTAL_REDEEMED));
        }

        int convertedThisMonth = 0;
        Map<String, Object> monthlyProps = new HashMap<>();
        monthlyProps.put(Constants.KARMA_POINTS_USER_ID, userId);
        monthlyProps.put(Constants.YEAR_MONTH, currentYearMonth);
        List<Map<String, Object>> monthlyRows = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER_KARMA_COIN_MONTHLY_SUMMARY, monthlyProps, new ArrayList<>());
        if (CollectionUtils.isNotEmpty(monthlyRows)) {
            convertedThisMonth = toInt(monthlyRows.get(0).get(Constants.POINTS_CONVERTED));
        }

        cacheWalletSummary(cacheKey, totalEarned, totalRedeemed, currentYearMonth, convertedThisMonth);
        return new WalletSnapshot(totalEarned, totalRedeemed, convertedThisMonth);
    }

    /**
     * Immutable holder for the coin figures loaded from cache/Cassandra, so callers read named
     * fields instead of positional array indices.
     */
    private static final class WalletSnapshot {
        private final int totalEarned;
        private final int totalRedeemed;
        private final int convertedThisMonth;

        WalletSnapshot(int totalEarned, int totalRedeemed, int convertedThisMonth) {
            this.totalEarned = totalEarned;
            this.totalRedeemed = totalRedeemed;
            this.convertedThisMonth = convertedThisMonth;
        }
    }

    private void cacheWalletSummary(String cacheKey, int totalEarned, int totalRedeemed, String yearMonth,
            int convertedThisMonth) {
        try {
            Map<String, Object> cacheMap = new HashMap<>();
            cacheMap.put(Constants.TOTAL_EARNED_CAMEL, totalEarned);
            cacheMap.put(Constants.TOTAL_REDEEMED_CAMEL, totalRedeemed);
            cacheMap.put(Constants.YEAR_MONTH_CAMEL, yearMonth);
            cacheMap.put(Constants.CONVERTED_THIS_MONTH, convertedThisMonth);
            redisCacheMgr.putStringInCache(cacheKey, objectMapper.writeValueAsString(cacheMap),
                    serverProperties.getKarmaCoinWalletRedisTtl());
        } catch (Exception e) {
            logger.error("Failed to cache karma coin wallet summary for key: " + cacheKey, e);
        }
    }

    private int fetchTotalKarmaPoints(String userId) {
        Map<String, Object> props = new HashMap<>();
        props.put(Constants.KARMA_POINTS_USER_ID, userId);
        List<Map<String, Object>> rows = cassandraOperation.getRecordsByProperties(Constants.KEYSPACE_SUNBIRD,
                Constants.TABLE_USER_KARMA_POINTS_SUMMARY, props, new ArrayList<>());
        if (CollectionUtils.isNotEmpty(rows)) {
            return toInt(rows.get(0).get(Constants.TOTAL_POINTS));
        }
        return 0;
    }

    private int toInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }

    @Override
    public SBApiResponse redeem(String token, Map<String, Object> requestBody) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_KARMA_WALLET_REDEEM);
        Map<String, Object> tokenPayload = accessTokenValidator.extractTokenPayload(token);
        String userId = accessTokenValidator.getUserIdFromPayload(tokenPayload);
        if (StringUtils.isBlank(userId)) {
            setError(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.UNAUTHORIZED);
            return response;
        }
        List<String> userRoles = accessTokenValidator.getUserRolesFromPayload(tokenPayload);
        List<String> authorizedRoles = serverProperties.getKarmaCoinWalletAuthorizedRoles();
        if (CollectionUtils.isEmpty(userRoles) || userRoles.stream().noneMatch(authorizedRoles::contains)) {
            setError(response, Constants.UNAUTHORIZED_USER, HttpStatus.FORBIDDEN);
            return response;
        }
        Object requestObj = (requestBody == null) ? null : requestBody.get(Constants.REQUEST);
        if (!(requestObj instanceof Map)) {
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        Map<?, ?> request = (Map<?, ?>) requestObj;
        Object pointsValue = request.get(Constants.POINTS_TO_CONVERT);
        if (!(pointsValue instanceof Number)) {
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        int pointsToConvert = ((Number) pointsValue).intValue();
        if (pointsToConvert <= 0) {
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        Object requestIdValue = request.get(Constants.REQUEST_ID);
        String requestId = requestIdValue == null ? null : requestIdValue.toString().trim();
        if (StringUtils.isBlank(requestId)) {
            setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
            return response;
        }
        boolean claimApplied = false;
        try {
            String currentYearMonth = YearMonth.now().toString();
            WalletSnapshot snapshot = loadWalletAndMonthly(userId, currentYearMonth);
            int totalEarned = snapshot.totalEarned;
            int convertedThisMonth = snapshot.convertedThisMonth;
            int totalKarmaPoints = fetchTotalKarmaPoints(userId);
            int monthlyCap = serverProperties.getKarmaCoinMonthlyCap();
            int unredeemedKarmaPoints = Math.max(
                            0,
                            totalKarmaPoints - totalEarned);
            int remainingCap = Math.max(
                            0,
                            monthlyCap - convertedThisMonth);
            int convertibleThisMonth = Math.min(
                            remainingCap,
                            unredeemedKarmaPoints);
            if (pointsToConvert > convertibleThisMonth) {
                setError(response, Constants.MONTHLY_CAP_EXCEEDED, HttpStatus.BAD_REQUEST);
                return response;
            }

            String lookupKey = buildLookupKey(userId, requestId);
            Map<String, Object> lookupRow = new HashMap<>();
            lookupRow.put(Constants.USER_KARMA_COIN_KEY, lookupKey);
            lookupRow.put(Constants.DB_COLUMN_OPERATION_TYPE, Constants.TXN_TYPE_CREDIT);
            lookupRow.put(Constants.DB_COLUMN_CREDIT_DATE, System.currentTimeMillis());
            Map<String, Object> claimAddInfo = new HashMap<>();
            claimAddInfo.put(Constants.STATUS, Constants.PROCESSING);
            lookupRow.put(Constants.ADDINFO, objectMapper.writeValueAsString(claimAddInfo));

            claimApplied = cassandraOperation.insertRecordIfNotExists(Constants.KEYSPACE_SUNBIRD, Constants.USER_KARMA_COIN_LOOKUP, lookupRow);
            if (!claimApplied) {
                Map<String, Object> existing = readLookupStatus(userId, requestId);
                Map<String, Object> result = (existing != null) ? existing : new HashMap<>();
                result.putIfAbsent(Constants.REQUEST_ID, requestId);
                result.putIfAbsent(Constants.STATUS, Constants.PROCESSING);
                response.setResult(result);
                response.getParams().setStatus(Constants.OK);
                response.setResponseCode(HttpStatus.OK);
                return response;
            }

            Map<String, Object> data = new HashMap<>();
            data.put(Constants.EVENT_EID, Constants.KARMA_COIN_CREDIT_EID);
            data.put(Constants.EVENT_ETS, System.currentTimeMillis());
            data.put(Constants.USER_ID, userId);
            data.put(Constants.REQUEST_ID, requestId);
            data.put(Constants.OPERATION, Constants.TXN_TYPE_CREDIT);
            data.put(Constants.ACTION_TYPE_CAMEL, Constants.POINTS_CONVERSION);
            data.put(Constants.POINTS_TO_CONVERT, pointsToConvert);
            data.put(Constants.CONTEXT_TYPE, Constants.POINTS_CONVERSION);
            data.put(Constants.CONTEXT_ID_CAMEL, requestId);
            data.put(Constants.CONVERSION_PERIOD, currentYearMonth);

            Map<String, Object> event = new HashMap<>();
            event.put(Constants.EVENT_TYPE, Constants.POINTS_CONVERSION);
            event.put(Constants.DATA, data);
            kafkaProducer.push(serverProperties.getKarmaCoinWalletRedeemTopic(), userId, event);
            Map<String, Object> result = new HashMap<>();
            result.put(Constants.REQUEST_ID, requestId);
            result.put(Constants.STATUS, Constants.PROCESSING);
            response.setResult(result);
            response.getParams().setStatus(Constants.ACCEPTED);
            response.setResponseCode(HttpStatus.ACCEPTED);
        } catch (Exception e) {
            logger.error("Failed to process karma coin redemption for user: {}", userId, e);
            if (claimApplied) {
                markRedeemFailed(userId, requestId, Constants.FAILED_TO_PROCESS_KARMA_COIN_REDEMPTION);
            }
            setError(response, Constants.FAILED_TO_PROCESS_KARMA_COIN_REDEMPTION, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    private String buildLookupKey(String userId, String requestId) {
        return userId + "|" + Constants.POINTS_CONVERSION + "|" + requestId;
    }

    /**
     * Reads the current redeem status from {@code user_karma_coin_lookup} for the given request.
     * Returns {@code null} when no row exists, otherwise a result map seeded with {@code requestId}
     * and populated from the stored {@code addinfo} JSON (falling back to {@code PROCESSING}).
     */
    private Map<String, Object> readLookupStatus(String userId, String requestId) throws java.io.IOException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(Constants.USER_KARMA_COIN_KEY, buildLookupKey(userId, requestId));
        properties.put(Constants.DB_COLUMN_OPERATION_TYPE, Constants.TXN_TYPE_CREDIT);
        List<Map<String, Object>> records = cassandraOperation.getRecordsByProperties(
                Constants.KEYSPACE_SUNBIRD, Constants.USER_KARMA_COIN_LOOKUP, properties, null);
        if (CollectionUtils.isEmpty(records)) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        result.put(Constants.REQUEST_ID, requestId);
        String addInfo = (String) records.get(0).get(Constants.ADDINFO);
        if (StringUtils.isBlank(addInfo)) {
            result.put(Constants.STATUS, Constants.PROCESSING);
        } else {
            Map<String, Object> statusData = objectMapper.readValue(addInfo, Map.class);
            result.putAll(statusData);
        }
        return result;
    }

    /**
     * Best-effort overwrite of the lookup row to a FAILED terminal state. Used when the API claimed
     * the request but could not publish the event, so polling clients get a terminal answer.
     */
    private void markRedeemFailed(String userId, String requestId, String errorMessage) {
        try {
            Map<String, Object> row = new HashMap<>();
            row.put(Constants.USER_KARMA_COIN_KEY, buildLookupKey(userId, requestId));
            row.put(Constants.DB_COLUMN_OPERATION_TYPE, Constants.TXN_TYPE_CREDIT);
            row.put(Constants.DB_COLUMN_CREDIT_DATE, System.currentTimeMillis());
            Map<String, Object> addInfo = new HashMap<>();
            addInfo.put(Constants.STATUS, Constants.FAILED_UPPERCASE);
            addInfo.put(Constants.REQUEST_ID, requestId);
            addInfo.put(Constants.ERROR_MESSAGE_CAMEL, errorMessage);
            row.put(Constants.ADDINFO, objectMapper.writeValueAsString(addInfo));
            cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_KARMA_COIN_LOOKUP, row);
        } catch (Exception ex) {
            logger.error("Failed to mark karma coin redemption FAILED for requestId: {}", requestId, ex);
        }
    }

    @Override
    public SBApiResponse getRedeemStatus(String token, String requestId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_KARMA_WALLET_REDEEM_STATUS);
        try {
            Map<String, Object> tokenPayload = accessTokenValidator.extractTokenPayload(token);
            String userId = accessTokenValidator.getUserIdFromPayload(tokenPayload);
            if (StringUtils.isBlank(userId)) {
                setError(response, Constants.USER_ID_DOESNT_EXIST, HttpStatus.UNAUTHORIZED);
                return response;
            }
            List<String> userRoles = accessTokenValidator.getUserRolesFromPayload(tokenPayload);
            List<String> authorizedRoles = serverProperties.getKarmaCoinWalletAuthorizedRoles();
            if (CollectionUtils.isEmpty(userRoles) || userRoles.stream().noneMatch(authorizedRoles::contains)) {
                setError(response, Constants.UNAUTHORIZED_USER, HttpStatus.FORBIDDEN);
                return response;
            }
            if (StringUtils.isBlank(requestId)) {
                setError(response, Constants.INVALID_REQUEST, HttpStatus.BAD_REQUEST);
                return response;
            }

            Map<String, Object> result = readLookupStatus(userId, requestId);
            if (MapUtils.isEmpty(result)) {
                setError(response, Constants.REDEEM_REQUEST_NOT_FOUND, HttpStatus.NOT_FOUND);
                return response;
            }
            response.setResult(result);
            response.getParams().setStatus(Constants.OK);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception e) {
            logger.error("Failed to fetch karma coin redemption status for requestId: {}", requestId, e);
            setError(response, "Failed to fetch karma coin redemption status", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }
}