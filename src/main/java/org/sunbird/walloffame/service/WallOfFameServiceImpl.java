package org.sunbird.walloffame.service;

import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.walloffame.entity.*;
import org.sunbird.walloffame.repository.*;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author mahesh.vakkund
 */
@Service
public class WallOfFameServiceImpl implements WallOfFameService {
    @Autowired
    private CassandraOperation cassandraOperation;

    private Logger logger = LoggerFactory.getLogger(getClass().getName());
    @Autowired
    AccessTokenValidator accessTokenValidator;

    @Autowired
    CbExtServerProperties properties;

    @Autowired
    private UserLeaderboardRepository leaderboardRepository;

    @Autowired
    private SlwMdoTopLearnerRepository slwMdoTopLearnerRepository;

    @Autowired
    private SlwMdoLeaderBoardRepository slwMdoLeaderBoardRepository;

    @Override
    public Map<String, Object> fetchWallOfFameData() {
        Map<String, Object> resultMap = new HashMap<>();
        LocalDate currentDate = LocalDate.now();
        LocalDate lastMonthDate = LocalDate.now().minusMonths(1);
        int lastMonthValue = lastMonthDate.getMonthValue();
        int lastMonthYearValue = lastMonthDate.getYear();

        String formattedDateLastMonth = lastMonthDate.format(DateTimeFormatter.ofPattern("MMMM yyyy"));
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.MONTH, lastMonthValue);
        propertyMap.put(Constants.YEAR, lastMonthYearValue);

        List<Map<String, Object>> dptList = new ArrayList<>();
        while (dptList.isEmpty()) {
            dptList = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.MDO_KARMA_POINTS, propertyMap, null);
            if (dptList.isEmpty()) {
                lastMonthDate = lastMonthDate.minusMonths(1);
                lastMonthValue = lastMonthDate.getMonthValue();
                lastMonthYearValue = lastMonthDate.getYear();
                formattedDateLastMonth = lastMonthDate.format(DateTimeFormatter.ofPattern("MMMM yyyy"));
                propertyMap.clear();
                propertyMap.put(Constants.MONTH, lastMonthValue);
                propertyMap.put(Constants.YEAR, lastMonthYearValue);
            }
        }
        resultMap.put(Constants.MDO_LIST, dptList);
        resultMap.put(Constants.TITLE, formattedDateLastMonth);
        return resultMap;
    }

    public SBApiResponse learnerLeaderBoard(String rootOrgId, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.LEARNER_LEADER_BOARD);
        try {
            if (StringUtils.isEmpty(rootOrgId)) {
                setBadRequestResponse(response, Constants.ORG_ID_MISSING);
                return response;
            }

            String userId = validateAuthTokenAndFetchUserId(authToken);
            if (StringUtils.isBlank(userId)) {
                setBadRequestResponse(response, Constants.USER_ID_DOESNT_EXIST);
                return response;
            }

            Map<String, Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constants.USER_ID_LOWER, userId);

            List<Map<String, Object>> userRowNum = cassandraOperation.getRecordsByProperties(
                    Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.TABLE_LEARNER_LEADER_BOARD_LOOK_UP,
                    propertiesMap,
                    null
            );

            if (userRowNum == null || userRowNum.isEmpty()) {
                response.put(Constants.RESULT, ListUtils.EMPTY_LIST);
                response.put(Constants.COUNT, 0);
                return response;
            }

            int res = (Integer) userRowNum.get(0).get(Constants.DB_COLUMN_ROW_NUM);
            List<Integer> ranksFilter = Arrays.asList(1, 2, 3, res - 1, res, res + 1);
            Map<String, Object> propMap = new HashMap<>();
            propMap.put(Constants.DB_COLUMN_ROW_NUM, ranksFilter);
            propMap.put(Constants.ORGID, rootOrgId);

            List<Map<String, Object>> result = cassandraOperation.getRecordsByProperties(
                    Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.TABLE_LEARNER_LEADER_BOARD,
                    propMap,
                    null
            );

            response.put(Constants.RESULT, result);
            response.put(Constants.COUNT, result == null ? 0 : result.size());
            return response;

        } catch (Exception e) {
            setInternalServerErrorResponse(response);
        }

        return response;
    }

    @Override
    public SBApiResponse fetchingTop10Learners(String ministryOrgId, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.TOP_10_LEARNERS);
        try {
            if (StringUtils.isEmpty(ministryOrgId)) {
                setBadRequestResponse(response, Constants.ORG_ID_MISSING);
                return response;
            }
            String userId = validateAuthTokenAndFetchUserId(authToken);
            if (StringUtils.isBlank(userId)) {
                setBadRequestResponse(response, Constants.USER_ID_DOESNT_EXIST);
                return response;
            }
            Map<String, Object> propMap = new HashMap<>();
            List<Integer> ranksFilter = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            propMap.put(Constants.DB_COLUMN_ROW_NUM, ranksFilter);
            propMap.put(Constants.ORGID, ministryOrgId);

            List<Map<String, Object>> result = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.SUNBIRD_KEY_SPACE_NAME,
                    Constants.TABLE_TOP_10_LEARNER,
                    propMap,
                    null
            );
            response.put(Constants.RESULT, result);
            return response;

        } catch (Exception e) {
            logger.error("Error while fetching top 10 learners: {}", e.getMessage(), e);
            setInternalServerErrorResponse(response);
        }
        return response;
    }

    @Override
    public SBApiResponse getUserLeaderBoard(String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_WALL_OF_FAME_USER_READ);
        String userId = validateAuthTokenAndFetchUserId(authToken);
        if (StringUtils.isBlank(userId)) {
            setBadRequestResponse(response, Constants.USER_ID_DOESNT_EXIST);
            return response;
        }
        try {
            Optional<UserLeaderboardEntity> entityOpt = leaderboardRepository.findById(userId);
            List<Map<String, Object>> userLeaderBoard = new ArrayList<>();

            if (entityOpt.isPresent()) {
                UserLeaderboardEntity entity = entityOpt.get();
                Map<String, Object> row = new HashMap<>();
                row.put(Constants.USER_ID, entity.getUserId());
                row.put(Constants.ROW_NUM, entity.getRowNum());
                row.put(Constants.COUNT, entity.getCount());
                row.put(Constants.DESIGNATION, entity.getDesignation());
                row.put(Constants.USER_FULL_NAME, entity.getFullName());
                row.put(Constants.LAST_CREDIT_DATE, entity.getLastCreditDate());
                row.put(Constants.ORG_ID, entity.getOrgId());
                row.put(Constants.PROFILE_IMAGE, entity.getProfileImage());
                row.put(Constants.RANK, entity.getRank());
                row.put(Constants.TOTAL_LEARNING_HOURS, entity.getTotalLearningHours());
                row.put(Constants.TOTAL_POINTS, entity.getTotalPoints());

                userLeaderBoard.add(row); // mimic Cassandra list of maps
            }
            if (CollectionUtils.isEmpty(userLeaderBoard)) {
                response.getParams().setErrmsg(Constants.NO_DATA_FOUND_FOR_THE_USER);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            } else {
                response.getParams().setStatus(Constants.SUCCESS);
                response.put(Constants.USER_LEADERBOARD, userLeaderBoard.get(0));
                response.setResponseCode(HttpStatus.OK);
            }
        } catch (Exception e) {
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrmsg(Constants.ERROR_WHILE_PROCESSING_USER_LEADERBOARD);
            response.getParams().setStatus(Constants.FAILED);
            logger.error("failed to process userLeaderBoard :: {}", String.valueOf(e));
        }
        return response;
    }

    @Override
    public SBApiResponse getMdoLeaderBoard() {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_WALL_OF_FAME_MDO_LEADERBOARD);
        Map<String, Object> propertyMap = new HashMap<>();
        propertyMap.put(Constants.SIZE, properties.getMdoLeaderBoardSizeList());
        try {
            List<Map<String, Object>> mdoLeaderBoard = cassandraOperation.getRecordsByPropertiesWithoutFiltering(
                    Constants.KEYSPACE_SUNBIRD, Constants.NLW_MDO_LEADERBOARD, propertyMap, null);
            if (CollectionUtils.isEmpty(mdoLeaderBoard)) {
                response.getParams().setErrmsg(Constants.NO_DATA_FOUND);
                response.getParams().setStatus(Constants.SUCCESS);
                response.setResponseCode(HttpStatus.OK);
            } else {
                response.getParams().setStatus(Constants.SUCCESS);
                response.put(Constants.MDO_LEADERBOARD, mdoLeaderBoard);
                response.setResponseCode(HttpStatus.OK);
            }
        } catch (Exception e) {
            response.getParams().setErrmsg(Constants.ERROR_WHILE_PROCESSING_MDO_LEADERBOARD);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
            logger.error("failed to process mdoLeaderBoard :: {}", String.valueOf(e));
        }
        return response;
    }

    @Override
    public SBApiResponse getStateMdoLeaderBoard(Map<String, Object> request) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.API_WALL_OF_FAME_STATE_MDO_LEADERBOARD);
        Map<String, Object> requestData = (Map<String, Object>) request.get(Constants.REQUEST);
        String orgId = (String) requestData.get(Constants.MDOID);
        List<String> sizeList = properties.getStateMdoLeaderBoardSizeList();
        try {

            List<SlwMdoLeaderBoardEntity> leaderboardList = slwMdoLeaderBoardRepository.findByParentIdAndSizeIn(orgId, sizeList );
            if (CollectionUtils.isEmpty(leaderboardList)) {
                response.getParams().setErrmsg(Constants.NO_DATA_FOUND);
            } else {
                List<Map<String, Object>> mdoLeaderBoard = leaderboardList.stream().map(record -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put(Constants.PARENT_ID, record.getParentId());
                    map.put(Constants.SIZE, record.getSize());
                    map.put(Constants.ROW_NUM, record.getRowNum());
                    map.put(Constants.ORG_ID, record.getOrgId());
                    map.put(Constants.ORG_NAME, record.getOrgName());
                    map.put(Constants.TOTAL_USERS, record.getTotalUsers());
                    map.put(Constants.TOTAL_LEARNING_HOURS, record.getTotalLearningHours());
                    return map;
                }).collect(Collectors.toList());
                response.put(Constants.MDO_LEADERBOARD, mdoLeaderBoard);
            }
        } catch (Exception e) {
            response.getParams().setErrmsg(String.format("%s , %s", Constants.ERROR_WHILE_PROCESSING_MDO_LEADERBOARD, e.getMessage()));            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setStatus(Constants.FAILED);
            logger.error("failed to process mdoLeaderBoard :: "+e.getMessage(), e);
        }
        return response;
    }

    @Override
    public SBApiResponse fetchingStateTop10Learners(String stateOrgId, String authToken) {
        SBApiResponse response = ProjectUtil.createDefaultResponse(Constants.STATE_TOP_10_LEARNERS);
        try {
            if (StringUtils.isEmpty(stateOrgId)) {
                setBadRequestResponse(response, Constants.ORG_ID_MISSING);
                return response;
            }
            String userId = validateAuthTokenAndFetchUserId(authToken);
            if (StringUtils.isBlank(userId)) {
                setBadRequestResponse(response, Constants.USER_ID_DOESNT_EXIST);
                return response;
            }
            List<Integer> ranksFilter = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
            List<SlwMdoTopLearnerEntity> learners = slwMdoTopLearnerRepository.findByOrgIdAndRowNumIn(stateOrgId, ranksFilter);
            if (CollectionUtils.isEmpty(learners)) {
                response.getParams().setErrmsg(Constants.NO_DATA_FOUND);
            }
            List<Map<String, Object>> result = convertToResponseMap(learners);
            response.put(Constants.RESULT, result);
            return response;

        } catch (Exception e) {
            setInternalServerErrorResponse(response);
            response.getParams().setErrmsg(e.getMessage());
            logger.error("failed to process top learners :: "+e.getMessage() , e);
        }
        return response;
    }

    private void setBadRequestResponse(SBApiResponse response, String errMsg) {
        response.getParams().setStatus(Constants.FAILED);
        response.getParams().setErrmsg(errMsg);
        response.setResponseCode(HttpStatus.BAD_REQUEST);
    }

    private void setInternalServerErrorResponse(SBApiResponse response) {
        response.getParams().setStatus(Constants.FAILED);
        response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private String validateAuthTokenAndFetchUserId(String authUserToken) {
        return accessTokenValidator.fetchUserIdFromAccessToken(authUserToken);
    }

    private List<Map<String, Object>> convertToResponseMap(List<SlwMdoTopLearnerEntity> learners) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (SlwMdoTopLearnerEntity learner : learners) {
            Map<String, Object> map = new HashMap<>();
            map.put(Constants.ORG_ID, learner.getOrgId());
            map.put(Constants.ROW_NUM, learner.getRowNum());
            map.put(Constants.USER_ID, learner.getUserId());
            map.put(Constants.USER_FULL_NAME, learner.getFullName());
            map.put(Constants.DESIGNATION, learner.getDesignation());
            map.put(Constants.PROFILE_IMAGE, learner.getProfileImage());
            map.put(Constants.TOTAL_POINTS, learner.getTotalPoints());
            map.put(Constants.ORG_NAME,learner.getOrgName());
            map.put(Constants.TOTAL_LEARNING_HOURS, learner.getTotalLearningHours());
            result.add(map);
        }

        return result;
    }

}
