package org.sunbird.recommendation.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.recommendation.dto.RecommendationDto;

import java.util.*;

@Slf4j
@Service
public class RecommendationServiceImpl implements RecommendationService {

    @Autowired
    CassandraOperation cassandraOperation;



    @Override
    public SBApiResponse getRecommendations(String userId) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COURSE_RECOMMENDATION_READ);
        if (StringUtils.isBlank(userId)) {
            response.getParams().setErrmsg(Constants.USER_ID_DOESNT_EXIST);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID_LOWER, userId);
            List<Map<String, Object>> courseRecommendations = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.USER_NON_RELEVANT_RECOMMENDATIONS,
                    propertyMap,
                    null
            );
            if (!courseRecommendations.isEmpty()) {
                for (Map<String, Object> courseRecommendation : courseRecommendations) {
                    if (!courseRecommendation.isEmpty()) {
                        response.setResponseCode(HttpStatus.OK);
                        response.setResult(courseRecommendation);
                    }
                }
            }
        }catch (Exception e){
            log.error("Exception occurred while fetching recommendations: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
        return response;
    }

    @Override
    public SBApiResponse saveRecommendations(RecommendationDto recommendationDto) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_COURSE_RECOMMENDATION_SAVE);
        if (recommendationDto.getUserId().isEmpty() || recommendationDto.getCourseIds().isEmpty()) {
            response.getParams().setErrmsg(Constants.USER_ID_COURSE_IDs_DOESNT_EXIST);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return response;
        }
        try {
            Map<String, Object> propertyMap = new HashMap<>();
            propertyMap.put(Constants.USER_ID_LOWER, recommendationDto.getUserId());

            List<Map<String, Object>> existingRecords = cassandraOperation.getRecordsByProperties(
                    Constants.KEYSPACE_SUNBIRD,
                    Constants.USER_NON_RELEVANT_RECOMMENDATIONS,
                    propertyMap,
                    null
            );
            Map<String, Object> userCourseEnrollMap = new HashMap<>();
            userCourseEnrollMap.put(Constants.USER_ID_LOWER, recommendationDto.getUserId());
            userCourseEnrollMap.put(Constants.UPDATED_ON_KEY,  new Date());

            if (existingRecords.isEmpty()) {
                userCourseEnrollMap.put(Constants.CREATED_ON_KEY, new Date());
                userCourseEnrollMap.put(Constants.COURSE_RECOMMENDATION, recommendationDto.getCourseIds());
            } else {
                Map<String, Object> existingRecord = existingRecords.get(0);
                List<String> existingCourseIds = (List<String>) existingRecord.get(Constants.COURSE_RECOMMENDATION);
                Set<String> updatedCourseIds = new HashSet<>(existingCourseIds);
                updatedCourseIds.addAll(recommendationDto.getCourseIds());
                userCourseEnrollMap.put(Constants.COURSE_RECOMMENDATION, new ArrayList<>(updatedCourseIds));
                userCourseEnrollMap.put(Constants.CREATED_ON_KEY, existingRecord.get(Constants.CREATED_ON));
            }

            SBApiResponse insertResponse = cassandraOperation.insertRecord(Constants.KEYSPACE_SUNBIRD, Constants.USER_NON_RELEVANT_RECOMMENDATIONS, userCourseEnrollMap);

            if (!Constants.SUCCESS.equalsIgnoreCase((String) insertResponse.get(Constants.RESPONSE))) {
                log.error("Failed to update database");
                response.getParams().setStatus(Constants.FAILED);
                response.getParams().setErrmsg("Failed to update database");
                response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
                return response;
            }
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.setResult(userCourseEnrollMap);
            return response;
        }catch (Exception e){
            log.error("Exception occurred while saving recommendations: ", e);
            response.getParams().setStatus(Constants.FAILED);
            response.getParams().setErrmsg(e.getMessage());
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            return response;
        }
    }
}
