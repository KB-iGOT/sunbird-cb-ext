package org.sunbird.recommendation.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.recommendation.dto.RecommendationDto;
import org.sunbird.recommendation.service.RecommendationService;

@RestController
@RequestMapping("/courseRecommendation")
public class RecommendationController {
    @Autowired
    private RecommendationService recommendationService;

    @GetMapping("/v1/read/{userId}")
    public ResponseEntity<SBApiResponse> getRecommendations(@PathVariable String userId) {
        SBApiResponse response = recommendationService.getRecommendations(userId);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping("/v1/update")
    public ResponseEntity<SBApiResponse> saveRecommendations(@RequestBody RecommendationDto recommendationDto) {
        SBApiResponse response = recommendationService.saveRecommendations(recommendationDto);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}
