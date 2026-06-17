package org.sunbird.insights.controller;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.model.SBApiResponse;
import static org.sunbird.common.util.Constants.*;
import static org.sunbird.common.util.ProjectUtil.updateErrorDetails;

import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.insights.controller.service.InsightsService;

import java.util.*;

@RestController
public class InsightsController {

    @Autowired
    private InsightsService insightsService;

    @Autowired
    AccessTokenValidator accessTokenValidator;

    @PostMapping("/user/v2/insights")
    public ResponseEntity<?> insights(
            @RequestBody Map<String, Object> requestBody,@RequestHeader("x-authenticated-userid") String userId) throws Exception {
        SBApiResponse response = insightsService.insights(requestBody,userId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/microsite/read/insights")
    public ResponseEntity<SBApiResponse> readInsights(
            @RequestBody Map<String, Object> requestBody,@RequestHeader(X_AUTH_USER_ID) String userId) throws Exception {
        SBApiResponse response = insightsService.readInsightsForOrganisation(requestBody,userId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/national/learning/week/insights")
    public ResponseEntity<SBApiResponse> getNationalLearningWeekInsights() {
        SBApiResponse response = insightsService.fetchNationalLearningData();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/recommendations/v1/courses")
    public ResponseEntity<SBApiResponse> getCourseRecommendationsByDesignation(
            @RequestHeader(X_AUTH_TOKEN) String authToken) {
        SBApiResponse response = insightsService.getCourseRecommendationsByDesignationV2(authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/state/learning/week/insights")
    public ResponseEntity<SBApiResponse> getStateLearningWeekInsights(@RequestBody Map<String, Object> request) {
        SBApiResponse response = insightsService.fetchStateLearningData(request);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/landingpage/insights")
    public ResponseEntity<?> landingPageMatrix() throws Exception {
        SBApiResponse response = insightsService.landingPageMatrix();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/volunteer/user/v2/insights")
    public ResponseEntity<?> volunteeInsights(
            @RequestBody Map<String, Object> requestBody,@RequestHeader(value = Constants.X_AUTH_TOKEN, required = true) String authToken) throws Exception {
        SBApiResponse response =  ProjectUtil.createDefaultResponse(API_USER_INSIGHTS);
        String userId = accessTokenValidator.fetchUserIdFromAccessToken(authToken);

        if (StringUtils.isNotBlank(userId)){
            response = insightsService.insights(requestBody,userId);
        }else{
            updateErrorDetails(response, UNAUTHORIZED, HttpStatus.UNAUTHORIZED);
        }
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
