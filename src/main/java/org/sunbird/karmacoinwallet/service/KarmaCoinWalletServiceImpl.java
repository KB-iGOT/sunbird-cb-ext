package org.sunbird.karmacoinwallet.service;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
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

@Service
public class KarmaCoinWalletServiceImpl implements KarmaCoinWalletService {

    private final CassandraOperation cassandraOperation;

    private final RedisCacheMgr redisCacheMgr;

    private final CbExtServerProperties serverProperties;

    private final AccessTokenValidator accessTokenValidator;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    public KarmaCoinWalletServiceImpl(CassandraOperation cassandraOperation, RedisCacheMgr redisCacheMgr,
            CbExtServerProperties serverProperties, AccessTokenValidator accessTokenValidator) {
        this.cassandraOperation = cassandraOperation;
        this.redisCacheMgr = redisCacheMgr;
        this.serverProperties = serverProperties;
        this.accessTokenValidator = accessTokenValidator;
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
}