package org.sunbird.profile.service;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.service.OutboundRequestHandlerServiceImpl;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;

import java.util.HashMap;
import java.util.Map;

@Service
public class OTPValidator {

    @Autowired
    private CbExtServerProperties serverConfig;

    @Autowired
    private OutboundRequestHandlerServiceImpl outboundRequestHandlerService;

    private Logger log = LoggerFactory.getLogger(getClass().getName());

    public boolean validateOTPForPersonalDetails(Map<String, Object> profileDetailsMap,
                                                  Map<String, Object> requestData,
                                                  SBApiResponse response, String userToken) {
        if (!profileDetailsMap.containsKey(Constants.PERSONAL_DETAILS)) {
            return true;
        }

        Map<String, Object> personalDetails = (Map<String, Object>) profileDetailsMap.get(Constants.PERSONAL_DETAILS);

        if (personalDetails.containsKey(Constants.MOBILE)) {
            String mobile = (String) personalDetails.get(Constants.MOBILE);

            if (!requestData.containsKey(Constants.PHONE_OTP)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Phone OTP is required for mobile number update");
                return false;
            }

            String phoneOtp = (String) requestData.get(Constants.PHONE_OTP);
            if (StringUtils.isBlank(phoneOtp)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Phone OTP is required for mobile number update");
                return false;
            }

            if (!verifyOTP(phoneOtp, Constants.PHONE, mobile, userToken)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Invalid phone OTP");
                return false;
            }else {
                requestData.remove(Constants.PHONE_OTP);
            }
        }

        if (personalDetails.containsKey(Constants.PRIMARY_EMAIL)) {
            String email = (String) personalDetails.get(Constants.PRIMARY_EMAIL);

            if (!requestData.containsKey(Constants.EMAIL_OTP)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Email OTP is required for email update");
                return false;
            }

            String emailOtp = (String) requestData.get(Constants.EMAIL_OTP);

            if (StringUtils.isBlank(emailOtp)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Email OTP is required for email update");
                return false;
            }

            if (!verifyOTP(emailOtp, Constants.EMAIL, email, userToken)) {
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Invalid email OTP");
                return false;
            }else {
                requestData.remove(Constants.EMAIL_OTP);
            }
        }
        return true;
    }

    private boolean verifyOTP(String otp, String type, String key, String userToken) {
        try {
            Map<String, Object> otpRequest = new HashMap<>();
            Map<String, Object> requestData = new HashMap<>();
            requestData.put(Constants.OTP, otp);
            requestData.put(Constants.TYPE, type);
            requestData.put(Constants.KEY, key);
            otpRequest.put(Constants.REQUEST, requestData);

            String url = serverConfig.getLearnerServiceHost() + serverConfig.getLmsOTPVerifyPath();
            Map<String, String> headers = new HashMap<>();
            headers.put(Constants.CONTENT_TYPE, Constants.APPLICATION_JSON);
            headers.put(Constants.X_AUTH_TOKEN, userToken);

            Map<String, Object> apiResponse = outboundRequestHandlerService.fetchResultUsingPost(url, otpRequest, headers);
            String responseCode = (String) apiResponse.get(Constants.RESPONSE_CODE);

            if (Constants.OK.equalsIgnoreCase(responseCode)) {
                log.info("OTP verification successful for type: {} and key: {}", type, key);
                return true;
            } else {
                log.warn("OTP verification failed for type: {} and key: {}. Response: {}", type, key, responseCode);
                return false;
            }
        } catch (Exception e) {
            log.error("Exception during OTP verification for type: {} and key: {}. Exception: ", type, key, e);
            return false;
        }
    }
}
