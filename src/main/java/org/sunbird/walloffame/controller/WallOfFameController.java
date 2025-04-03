package org.sunbird.walloffame.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.walloffame.service.WallOfFameService;

import java.util.Map;

/**
 * @author mahesh.vakkund & Deepak kr Thakur
 */
@RestController
public class WallOfFameController {
    @Autowired
    private WallOfFameService wallOfFameService;

    @PostMapping({"/v1/halloffame/read", "/v1/walloffame/read"})
    public ResponseEntity<Map<String, Object>> fetchWallOfFameData() {
        Map<String, Object> wallOfFameDataMap = wallOfFameService.fetchWallOfFameData();
        return new ResponseEntity<>(wallOfFameDataMap, HttpStatus.OK);
    }

    @GetMapping({"/v1/halloffame/learnerleaderboard", "/v1/walloffame/learnerleaderboard"})
    public ResponseEntity <SBApiResponse> learnerLeaderBoard
            (@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
             @RequestHeader(Constants.X_AUTH_USER_ORG_ID) String rootOrgId) throws Exception {
        SBApiResponse response = wallOfFameService.learnerLeaderBoard(rootOrgId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/top/learners/{ministryOrgId}")
    public ResponseEntity<SBApiResponse> fetchingTopLearners
            (@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
            @PathVariable String ministryOrgId) throws Exception {
        SBApiResponse response = wallOfFameService.fetchingTop10Learners(ministryOrgId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping({"v1/halloffame/user/read", "v1/walloffame/user/read"})
    public ResponseEntity<SBApiResponse> getUserLeaderBoard(@RequestHeader(Constants.X_AUTH_TOKEN) String authToken) {
        SBApiResponse response = wallOfFameService.getUserLeaderBoard(authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping({"v1/halloffame/mdoleaderboard", "v1/walloffame/mdoleaderboard"})
    public ResponseEntity<SBApiResponse> getMdoLeaderBoard() {
        SBApiResponse response = wallOfFameService.getMdoLeaderBoard();
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @PostMapping({"v1/halloffame/state/mdoleaderboard", "v1/walloffame/state/mdoleaderboard"})
    public ResponseEntity<SBApiResponse> getStateMdoLeaderBoard(@RequestBody Map<String, Object> request) {
        SBApiResponse response = wallOfFameService.getStateMdoLeaderBoard(request);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

    @GetMapping("/v1/state/top/learners/{stateOrgId}")
    public ResponseEntity<SBApiResponse> fetchingStateTopLearners
            (@RequestHeader(Constants.X_AUTH_TOKEN) String authToken,
             @PathVariable String stateOrgId) throws Exception {
        SBApiResponse response = wallOfFameService.fetchingStateTop10Learners(stateOrgId, authToken);
        return new ResponseEntity<>(response, response.getResponseCode());
    }

}
