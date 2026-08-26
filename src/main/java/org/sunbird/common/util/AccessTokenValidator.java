package org.sunbird.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.keycloak.common.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.sunbird.common.model.SBApiResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
@Component
public class AccessTokenValidator {

    @Autowired
    KeyManager keyManager;
    private static Logger logger = LoggerFactory.getLogger(AccessTokenValidator.class.getName());
    private static ObjectMapper mapper = new ObjectMapper();
    private static PropertiesCache cache = PropertiesCache.getInstance();

    private Map<String, Object> validateToken(String token) throws Exception {
        try {
            String[] tokenElements = token.split("\\.");
            String header = tokenElements[0];
            String body = tokenElements[1];
            String signature = tokenElements[2];
            String payLoad = header + Constants.DOT_SEPARATOR + body;
            Map<Object, Object> headerData =
                    mapper.readValue(new String(decodeFromBase64(header)), Map.class);
            String keyId = headerData.get("kid").toString();
            boolean isValid =
                    CryptoUtil.verifyRSASign(
                            payLoad,
                            decodeFromBase64(signature),
                            keyManager.getPublicKey(keyId).getPublicKey(),
                            Constants.SHA_256_WITH_RSA);
            if (isValid) {
                Map<String, Object> tokenBody =
                        mapper.readValue(new String(decodeFromBase64(body)), Map.class);
                boolean isExp = isExpired((Integer) tokenBody.get("exp"));
                if (isExp) {
                    throw new Exception("Expired auth token is received.");
                }
                return tokenBody;
            } else {
                throw new Exception("Invalid auth token is received.");
            }
        } catch (Exception e) {
            logger.warn("Failed to validate the user token. Exception: ", e);
        }
        return Collections.EMPTY_MAP;
    }


    public String verifyUserToken(String token) {
        String userId = Constants._UNAUTHORIZED;
        try {
            Map<String, Object> payload = validateToken(token);
            if (MapUtils.isNotEmpty(payload) && checkIss((String) payload.get("iss"))) {
                userId = (String) payload.get(Constants.SUB);
                if (StringUtils.isNotBlank(userId)) {
                    int pos = userId.lastIndexOf(":");
                    userId = userId.substring(pos + 1);
                }
            }
        } catch (Exception ex) {
            logger.error("Exception in verifyUserAccessToken: verify ", ex);
        }
        return userId;
    }

	private boolean checkIss(String iss) {
		String realmUrl = cache.getProperty(Constants.SSO_URL) + "realms/" + cache.getProperty(Constants.SSO_REALM);
		if (StringUtils.isBlank(realmUrl))
			return false;
		return (realmUrl.equalsIgnoreCase(iss));
	}

    private boolean isExpired(Integer expiration) {
        int currentTime = Time.currentTime();
        boolean retValue = (currentTime > expiration);
        if (retValue) {
            logger.warn(String.format("Received expired auth token request. Current time: {}, Token expire time: {}",
                    currentTime, expiration));
        }
        return retValue;
    }

    private byte[] decodeFromBase64(String data) {
        return Base64Util.decode(data, 11);
    }

    public String fetchUserIdFromAccessToken(String accessToken) {
        String clientAccessTokenId = null;
        if (accessToken != null) {
            try {
                clientAccessTokenId = verifyUserToken(accessToken);
                if (Constants._UNAUTHORIZED.equalsIgnoreCase(clientAccessTokenId)) {
                    clientAccessTokenId = null;
                }
            } catch (Exception ex) {
                String errMsg = "Exception occurred while fetching the userid from the access token. Exception: " + ex.getMessage();
                logger.error(errMsg, ex);
                clientAccessTokenId = null;
            }
        }
        return clientAccessTokenId;
    }

    public String fetchUserIdFromAccessToken(String accessToken, SBApiResponse response) {
        String clientAccessTokenId = null;
        if (accessToken != null) {
            try {
                clientAccessTokenId = verifyUserToken(accessToken);
                if (Constants._UNAUTHORIZED.equalsIgnoreCase(clientAccessTokenId)) {
                    response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrmsg(Constants.ACCESS_TOKEN_IS_EXPIRED);
                    response.setResponseCode(HttpStatus.UNAUTHORIZED);
                    clientAccessTokenId = null;
                }
            } catch (Exception ex) {
                String errMsg = "Exception occurred while fetching the userid from the access token. Exception: " + ex.getMessage();
                logger.error(errMsg, ex);
                response.getParams().setStatus(Constants.FAILED);
                    response.getParams().setErrmsg(Constants.ACCESS_TOKEN_VALIDATION_FAILED);
                    response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                clientAccessTokenId = null;
            }
        }
        return clientAccessTokenId;
    }

    public Map<String, Object> extractTokenPayload(String token) {
        Map<String, Object> tokenPayload = new HashMap<>();
        try {
            Map<String, Object> payload = validateToken(token);
            if (MapUtils.isNotEmpty(payload) && checkIss((String) payload.get("iss"))) {
                tokenPayload = payload;
            }
        } catch (Exception ex) {
            logger.error("Exception in extractTokenPayload: ", ex);
        }
        return tokenPayload;
    }

    /**
     * Extracts the userId from an already-validated token payload (see {@link #extractTokenPayload(String)}),
     * mirroring the {@code sub}-claim handling in {@link #verifyUserToken(String)}. Use this to avoid
     * validating the same token more than once when both the userId and roles are needed.
     */
    public String getUserIdFromPayload(Map<String, Object> tokenPayload) {
        if (MapUtils.isEmpty(tokenPayload)) {
            return null;
        }
        String userId = (String) tokenPayload.get(Constants.SUB);
        if (StringUtils.isNotBlank(userId)) {
            userId = userId.substring(userId.lastIndexOf(":") + 1);
        }
        return userId;
    }

    /**
     * Extracts the user roles from an already-validated token payload (see
     * {@link #extractTokenPayload(String)}). Returns an empty list when no roles are present.
     */
    @SuppressWarnings("unchecked")
    public List<String> getUserRolesFromPayload(Map<String, Object> tokenPayload) {
        if (MapUtils.isEmpty(tokenPayload)) {
            return Collections.emptyList();
        }
        Object rolesClaim = tokenPayload.get(Constants.USER_ROLES_KEY);
        if (!(rolesClaim instanceof List)) {
            return Collections.emptyList();
        }
        return (List<String>) rolesClaim;
    }

    public List<String> fetchUserRolesFromToken(String accessToken) {
        try {
            Map<String, Object> tokenBody = validateToken(accessToken);
            if (MapUtils.isEmpty(tokenBody)) {
                return Collections.emptyList();
            }
            List<String> roles = (List<String>) tokenBody.get("user_roles");
            return roles != null ? roles : Collections.emptyList();
        } catch (Exception ex) {
            logger.error("Exception while fetching roles from token: {}", ex.getMessage());
            return Collections.emptyList();
        }
    }

}
