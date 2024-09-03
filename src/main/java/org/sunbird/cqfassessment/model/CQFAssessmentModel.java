package org.sunbird.cqfassessment.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author mahesh.vakkund
 */
public class CQFAssessmentModel {
    private  String userId;
    private  String assessmentIdentifier;
    private  String contentId;
    private  String versionKey;
    private  List<Map<String, Object>> sectionListFromSubmitRequest = new ArrayList<>();
    private  List<Map<String, Object>> hierarchySectionList = new ArrayList<>();
    private  Map<String, Object> assessmentHierarchy = new HashMap<>();
    private  Map<String, Object> existingAssessmentData = new HashMap<>();

    public CQFAssessmentModel() {
    }


    public CQFAssessmentModel(String userId, String assessmentIdentifier, String contentId, String versionKey) {
        this.userId = userId;
        this.assessmentIdentifier = assessmentIdentifier;
        this.contentId = contentId;
        this.versionKey = versionKey;
    }


    public CQFAssessmentModel(String userId, List<Map<String, Object>> sectionListFromSubmitRequest, Map<String, Object> assessmentHierarchy, List<Map<String, Object>> hierarchySectionList, Map<String, Object> existingAssessmentData, String errMsg) {
        this.userId = userId;
        this.sectionListFromSubmitRequest = sectionListFromSubmitRequest;
        this.assessmentHierarchy = assessmentHierarchy;
        this.hierarchySectionList = hierarchySectionList;
        this.existingAssessmentData = existingAssessmentData;;
    }

    public String getUserId() {
        return userId;
    }

    public String getAssessmentIdentifier() {
        return assessmentIdentifier;
    }

    public String getContentId() {
        return contentId;
    }

    public String getVersionKey() {
        return versionKey;
    }

    public List<Map<String, Object>> getSectionListFromSubmitRequest() {
        return sectionListFromSubmitRequest;
    }

    public List<Map<String, Object>> getHierarchySectionList() {
        return hierarchySectionList;
    }

    public Map<String, Object> getAssessmentHierarchy() {
        return assessmentHierarchy;
    }

    public Map<String, Object> getExistingAssessmentData() {
        return existingAssessmentData;
    }
}
