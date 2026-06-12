package org.sunbird.insights.controller.service;

import org.sunbird.common.model.SBApiResponse;

import java.io.IOException;
import java.util.Map;

public interface InsightsService {

    public SBApiResponse insights(Map<String, Object> requestBody,String userId, String weekRange) throws Exception;

    public SBApiResponse readInsightsForOrganisation(Map<String, Object> requestBody, String userId);

    public  SBApiResponse fetchNationalLearningData();

    public SBApiResponse getCourseRecommendationsByDesignation(String authToken);

    public  SBApiResponse fetchStateLearningData(Map<String,Object> request);

    public SBApiResponse getCourseRecommendationsByDesignationV2(String authToken);

    public SBApiResponse landingPageMatrix() throws IOException;

}
