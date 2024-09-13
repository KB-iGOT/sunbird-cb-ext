package org.sunbird.cqfassessment.controller;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.cqfassessment.service.CQFAssessmentService;

import javax.validation.Valid;
import java.util.Map;

/**
 * @author mahesh.vakkund
 */
@RestController
@RequestMapping("/v1/cqfassessment")
public class CQFAssessmentController {

    @Autowired
    private CQFAssessmentService cqfAssessmentService;

    @PostMapping("/create")
    public ResponseEntity<SBApiResponse> createCQFAssessment(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
                                                             @Valid @RequestBody Map<String, Object> requestBody) {
        SBApiResponse createdAssessment = cqfAssessmentService.createCQFAssessment(authToken, requestBody);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdAssessment);
    }

    @PutMapping("/update/{cqfAssessmentIdentifier}")
    public ResponseEntity<SBApiResponse> updateCQFAssessment(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
                                                             @Valid @RequestBody Map<String, Object> requestBody,
                                                             @PathVariable("cqfAssessmentIdentifier") String cqfAssessmentIdentifier) {

        SBApiResponse updatedAssessment = cqfAssessmentService.updateCQFAssessment(requestBody,authToken,cqfAssessmentIdentifier);
        return ResponseEntity.ok(updatedAssessment);
    }

    @GetMapping("/readEntry/{cqfAssessmentIdentifier}")
    public ResponseEntity<SBApiResponse> getCQFAssessment(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
                                                          @PathVariable("cqfAssessmentIdentifier") String cqfAssessmentIdentifier) {
        SBApiResponse assessment = cqfAssessmentService.getCQFAssessment(authToken,cqfAssessmentIdentifier);
        return ResponseEntity.ok(assessment);
    }

    @GetMapping("/list")
    public ResponseEntity<SBApiResponse> listCQFAssessments(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        SBApiResponse assessments = cqfAssessmentService.listCQFAssessments(authToken);
        return ResponseEntity.ok(assessments);
    }

    @GetMapping("/read/{assessmentIdentifier}/{contentId}/{versionKey}")
    public ResponseEntity<SBApiResponse> readAssessment(
            @PathVariable("assessmentIdentifier") String assessmentIdentifier,
            @PathVariable("contentId") String contentId,
            @PathVariable("versionKey") String versionKey,
            @RequestHeader(Constants.X_AUTH_TOKEN) String token, @RequestParam(name = "editMode", required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse readResponse = cqfAssessmentService.readAssessment(assessmentIdentifier, token, edit, contentId, versionKey);
        return new ResponseEntity<>(readResponse, readResponse.getResponseCode());
    }


    @PostMapping("/submit")
    public ResponseEntity<SBApiResponse> submitUserAssessmentV5(@Valid @RequestBody Map<String, Object> requestBody,
                                                                @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse submitResponse = cqfAssessmentService.submitCQFAssessment(requestBody, authUserToken,edit);
        return new ResponseEntity<>(submitResponse, submitResponse.getResponseCode());
    }

    @PostMapping("/result")
    public ResponseEntity<SBApiResponse> readAssessmentResultV5(@Valid @RequestBody Map<String, Object> requestBody,
                                                                @RequestHeader("x-authenticated-user-token") String authUserToken) {
        SBApiResponse response = cqfAssessmentService.readCQFAssessmentResult(requestBody, authUserToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }


    @PostMapping("/questionset/create")
    public ResponseEntity<SBApiResponse> createCQFQuestionSet(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
                                                              @Valid @RequestBody Map<String, Object> requestBody) {
        SBApiResponse createdAssessment = cqfAssessmentService.createCQFQuestionSet(authToken, requestBody);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdAssessment);
    }

    @PatchMapping("/questionset/update")
    public ResponseEntity<SBApiResponse> updateCQFQuestionSet(
            @RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
            @Valid @RequestBody Map<String, Object> requestBody) {
        SBApiResponse createdAssessment = cqfAssessmentService.updateCQFQuestionSet(authToken, requestBody);
        return ResponseEntity.status(HttpStatus.CREATED).body(createdAssessment);
    }


    @PostMapping("/question/list")
    public ResponseEntity<SBApiResponse> readQuestionList(@Valid @RequestBody Map<String, Object> requestBody,
                                                            @RequestHeader("x-authenticated-user-token") String authUserToken,@RequestParam(name = "editMode" ,required = false) String editMode) {
        boolean edit = !StringUtils.isEmpty(editMode) && Boolean.parseBoolean(editMode);
        SBApiResponse response = cqfAssessmentService.readQuestionList(requestBody, authUserToken,edit);
        return new ResponseEntity<>(response, response.getResponseCode());
    }
}