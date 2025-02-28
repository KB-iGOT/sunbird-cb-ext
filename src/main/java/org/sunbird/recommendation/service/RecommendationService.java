package org.sunbird.recommendation.service;


import org.springframework.stereotype.Service;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.recommendation.dto.RecommendationDto;

@Service
public interface RecommendationService {
    SBApiResponse getRecommendations(String userId);

    SBApiResponse saveRecommendations(RecommendationDto userCourseIds);
}
