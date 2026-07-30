package org.sunbird.programcoordinator.service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.model.SearchUserApiContent;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.ProjectUtil;
import org.sunbird.core.producer.Producer;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorEntity;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorRoleEntity;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorListDto;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorListItem;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRepository;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRoleRepository;
import org.sunbird.user.service.UserUtilityService;

import javax.annotation.PostConstruct;

import static org.sunbird.common.util.Constants.ERROR_REQUIRED_ROLE_PREFIX;
import static org.sunbird.common.util.Constants.PC_USER_PROFILE_CACHE_KEY;

@Service
public class ProgramCoordinatorServiceImpl implements ProgramCoordinatorService {

    private final Logger logger = LoggerFactory.getLogger(getClass().getName());

    private static final Map<String, String> SORTABLE_FIELDS = new HashMap<>();
    static {
        SORTABLE_FIELDS.put("roleId", "role_id");
        SORTABLE_FIELDS.put("userId", "user_id");
    }

    @Value("#{'${program.coordinator.allowed.roles}'.split(',')}")
    private List<String> allowedRoles;

    @Value("${program.coordinator.cache.key.prefix}")
    private String cacheKeyPrefix;

    @Value("${program.coordinator.sync.topic}")
    private String coordinatorSyncTopic;

    private Map<Short, String> roleMap;


    private ProgramCoordinatorRepository programCoordinatorRepository;
    private ProgramCoordinatorRoleRepository programCoordinatorRoleRepository;
    private AccessTokenValidator accessTokenValidator;
    private RedisCacheMgr redisCacheMgr;
    private Producer kafkaProducer;
    private ObjectMapper objectMapper;
    private UserUtilityService userUtilityService;

    public ProgramCoordinatorServiceImpl(ProgramCoordinatorRepository programCoordinatorRepository, ProgramCoordinatorRoleRepository programCoordinatorRoleRepository, AccessTokenValidator accessTokenValidator, RedisCacheMgr redisCacheMgr, Producer kafkaProducer, ObjectMapper objectMapper, UserUtilityService userUtilityService) {
        this.programCoordinatorRepository = programCoordinatorRepository;
        this.programCoordinatorRoleRepository = programCoordinatorRoleRepository;
        this.accessTokenValidator = accessTokenValidator;
        this.redisCacheMgr = redisCacheMgr;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = objectMapper;
        this.userUtilityService = userUtilityService;
    }

    @PostConstruct
    public void loadRoles() {
        roleMap = programCoordinatorRoleRepository.findAll()
                .stream()
                .collect(Collectors.toMap(
                        ProgramCoordinatorRoleEntity::getId,
                        ProgramCoordinatorRoleEntity::getRoleName));
    }

    @Override
    public SBApiResponse upsert(String programId, List<ProgramCoordinatorUpsertRequest> requests, String token) {

        SBApiResponse response = new SBApiResponse(Constants.API_PROGRAM_COORDINATOR_UPSERT);

        try {

            List<String> userRoles = accessTokenValidator.fetchUserRolesFromToken(token);

            boolean hasAccess = userRoles.stream().anyMatch(allowedRoles::contains);

            if (!hasAccess) {
                response.getParams().setErrmsg(
                        ERROR_REQUIRED_ROLE_PREFIX + String.join(", ", allowedRoles));
                response.setResponseCode(HttpStatus.FORBIDDEN);
                return response;
            }

            if (!validateUpsertRequest(requests, response)) {
                return response;
            }

            UUID actorUuid = UUID.randomUUID();

            List<String> addedOrUpdated = new ArrayList<>();
            List<String> removed = new ArrayList<>();
            Set<String> affectedUsers = new HashSet<>();

            for (ProgramCoordinatorUpsertRequest request : requests) {

                affectedUsers.add(request.getUserId().toString());

                if (request.getStatus().equals(Constants.ACTIVE_STATUS_PC)) {

                    int rows = programCoordinatorRepository.addOrResurrect(
                            programId,
                            request.getUserId(),
                            request.getRoleId(),
                            actorUuid);

                    if (rows > 0) {
                        addedOrUpdated.add(request.getUserId().toString());
                    }

                } else {

                    int rows = programCoordinatorRepository.softRemove(
                            programId,
                            request.getUserId(),
                            actorUuid);

                    if (rows > 0) {
                        removed.add(request.getUserId().toString());
                    }
                }
            }

            Map<String, Object> event = new HashMap<>();
            event.put(Constants.EVENT_TYPE, Constants.EVENT_TYPE_COORDINATOR_LIST_SYNCED);
            event.put(Constants.USER_IDS, affectedUsers);
            event.put(Constants.TIMESTAMP, Instant.now().toString());

            kafkaProducer.push(coordinatorSyncTopic, event);

            Map<String, Object> result = new HashMap<>();
            result.put(Constants.PROGRAM_ID, programId);
            result.put(Constants.ADDED_OR_UPDATED, addedOrUpdated);
            result.put(Constants.REMOVED, removed);

            response.put(Constants.RESPONSE, result);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);

        } catch (Exception ex) {
            logger.error("Error while upserting coordinators", ex);
            response.getParams().setErrmsg(Constants.INTERNAL_SERVER_ERROR);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return response;

    }

    @Override
    public SBApiResponse list(String programId, int limit, int offset, String sortBy, String sortDirection, String token) {
        SBApiResponse response = new SBApiResponse(Constants.API_PROGRAM_COORDINATOR_LIST);
        try {
            if (StringUtils.isEmpty(programId)) {
                response.getParams().setErrmsg("programId is required");
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            int safeLimit = limit > 0 ? limit : 20;
            int safeOffset = offset >= 0 ? offset : 0;
            int page = safeOffset / safeLimit;
            String sortColumn = SORTABLE_FIELDS.get(sortBy);

            Pageable pageable;
            if (sortColumn != null) {
                Sort.Direction direction = "desc".equalsIgnoreCase(sortDirection) ? Sort.Direction.DESC
                        : Sort.Direction.ASC;
                pageable = PageRequest.of(page, safeLimit, Sort.by(direction, sortColumn));
            } else {
                pageable = PageRequest.of(page, safeLimit);
            }

            Page<ProgramCoordinatorListDto> result = programCoordinatorRepository.findCoordinators(programId,
                    pageable);

            List<ProgramCoordinatorListDto> coordinators = result.getContent();
            List<String> userIds = coordinators.stream()
                    .map(item -> item.getUserId().toString())
                    .distinct()
                    .collect(Collectors.toList());
            Map<String, SearchUserApiContent> userProfiles = getUserProfiles(userIds, token);
            List<Map<String, Object>> content = new ArrayList<>();

            for (ProgramCoordinatorListDto item : coordinators) {

                Map<String, Object> coordinatorMap = new HashMap<>();

                coordinatorMap.put(Constants.USER_ID, item.getUserId());
                coordinatorMap.put(Constants.ROLE_ID, item.getRoleId());
                coordinatorMap.put(Constants.ROLE_NAME, item.getRoleName());

                SearchUserApiContent profile = userProfiles.get(item.getUserId().toString());

                populateCoordinatorProfile(coordinatorMap, profile);

                content.add(coordinatorMap);
            }

            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put(Constants.CONTENT, content);
            responseMap.put(Constants.COUNT, result.getTotalElements());
            responseMap.put(Constants.LIMIT, safeLimit);
            responseMap.put(Constants.OFFSET, safeOffset);

            response.put(Constants.RESPONSE, responseMap);

            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
        } catch (Exception ex) {
            String errMsg = "Exception occurred while listing program coordinators. Exception: " + ex.getMessage();
            logger.error(errMsg, ex);
            response.getParams().setErrmsg(errMsg);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return response;
    }

    @Override
    public SBApiResponse getProgramCoordinator(String programId, String token) {

        SBApiResponse response = new SBApiResponse(Constants.API_PROGRAM_COORDINATOR_READ);

        try {

            List<ProgramCoordinatorEntity> coordinators = programCoordinatorRepository.findActiveByProgramId(programId);

            if (CollectionUtils.isEmpty(coordinators)) {
                response.getParams().setStatus(Constants.FAILED);
                response.getResult().put(Constants.COORDINATORS, Collections.emptyList());
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return response;
            }

            List<String> userIds = coordinators.stream()
                    .map(pc -> pc.getUserId().toString())
                    .collect(Collectors.toList());

            Map<String, SearchUserApiContent> userProfiles =
                    getUserProfiles(userIds, token);

            List<Map<String, Object>> coordinatorResponse = new ArrayList<>();

            for (ProgramCoordinatorEntity coordinator : coordinators) {

                Map<String, Object> coordinatorMap = new HashMap<>();

                String userId = coordinator.getUserId().toString();

                coordinatorMap.put(Constants.USER_ID, userId);
                coordinatorMap.put(Constants.ROLE_ID, coordinator.getRoleId());
                coordinatorMap.put(Constants.TRAINER_TYPE, roleMap.get(coordinator.getRoleId()));

                populateCoordinatorProfile(
                        coordinatorMap,
                        userProfiles.get(userId));

                coordinatorResponse.add(coordinatorMap);
            }
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);
            response.getResult().put(Constants.COORDINATORS, coordinatorResponse);

        } catch (Exception e) {
            logger.error("Error while fetching program coordinators", e);
            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrmsg("Failed to fetch program coordinators.");
        }

        return response;
    }


    @Override
    public SBApiResponse getCoordinatorRoles(String authUserToken) {

        SBApiResponse response = new SBApiResponse(Constants.API_PROGRAM_COORDINATOR_ROLES);

        List<String> userRoles = accessTokenValidator.fetchUserRolesFromToken(authUserToken);

        boolean hasAccess = userRoles.stream().anyMatch(allowedRoles::contains);

        if (!hasAccess) {
            response.getParams().setErrmsg(
                    ERROR_REQUIRED_ROLE_PREFIX + String.join(", ", allowedRoles));
            response.setResponseCode(HttpStatus.FORBIDDEN);
            return response;
        }

        try {

            List<Map<String, Object>> roles = new ArrayList<>();

            roleMap.forEach((roleId, roleName) -> {
                Map<String, Object> role = new HashMap<>();
                role.put(Constants.ROLE_ID, roleId);
                role.put(Constants.ROLE_NAME, roleName);
                roles.add(role);
            });

            response.getResult().put(Constants.ROLES, roles);
            response.getParams().setStatus(Constants.SUCCESSFUL);
            response.setResponseCode(HttpStatus.OK);

        } catch (Exception e) {

            logger.error("Error while fetching coordinator roles", e);

            response.getParams().setStatus(Constants.FAILED);
            response.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
            response.getParams().setErrmsg("Failed to fetch coordinator roles.");
        }

        return response;
    }

    private boolean validateUpsertRequest(List<ProgramCoordinatorUpsertRequest> requests,
                                          SBApiResponse response) {

        if (CollectionUtils.isEmpty(requests)) {
            response.getParams().setErrmsg(Constants.COORDINATOR_LIST_REQUIRED);
            response.setResponseCode(HttpStatus.BAD_REQUEST);
            return false;
        }

        Set<UUID> userIds = new HashSet<>();

        for (ProgramCoordinatorUpsertRequest request : requests) {

            if (request.getUserId() == null) {
                response.getParams().setErrmsg(Constants.USER_ID_REQUIRED);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return false;
            }

            if (request.getStatus() == null
                    || (!request.getStatus().equals(Constants.ACTIVE_STATUS_PC)
                    && !request.getStatus().equals(Constants.INACTIVE_STATUS_PC))) {

                response.getParams().setErrmsg(Constants.INVALID_STATUS);
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return false;
            }

            if (request.getStatus().equals(Constants.ACTIVE_STATUS_PC)) {

                if (request.getRoleId() == null) {
                    response.getParams().setErrmsg(Constants.ROLE_ID_REQUIRED);
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return false;
                }

                if (!programCoordinatorRoleRepository.existsById(request.getRoleId())) {
                    response.getParams().setErrmsg(
                            Constants.INVALID_ROLE_ID + request.getRoleId());
                    response.setResponseCode(HttpStatus.BAD_REQUEST);
                    return false;
                }
            }

            if (!userIds.add(request.getUserId())) {
                response.getParams().setErrmsg(
                        Constants.DUPLICATE_COORDINATOR + request.getUserId());
                response.setResponseCode(HttpStatus.BAD_REQUEST);
                return false;
            }
        }

        return true;
    }

    private void populateCoordinatorProfile(
            Map<String, Object> coordinatorMap,
            SearchUserApiContent profile) {

        if (profile == null || profile.getProfileDetails() == null
                || profile.getProfileDetails().getPersonalDetails() == null) {
            return;
        }

        Map<String, Object> personalDetails =
                profile.getProfileDetails().getPersonalDetails();

        Object firstName = personalDetails.get("firstname");
        if (firstName != null) {
            coordinatorMap.put(Constants.FIRST_NAME, firstName.toString());
        }

        Object primaryEmail = personalDetails.get(Constants.PRIMARY_EMAIL);
        if (primaryEmail != null) {
            coordinatorMap.put(Constants.EMAIL, primaryEmail.toString());
        }
    }

    private Map<String, SearchUserApiContent> getUserProfiles(
            List<String> userIds, String token) throws Exception {

        Map<String, SearchUserApiContent> userProfiles = new HashMap<>();
        List<String> missedUsers = new ArrayList<>();

        for (String userId : userIds) {

            String redisKey = Constants.PC_USER_PROFILE_CACHE_KEY + userId;

            String cachedProfile = redisCacheMgr.getCache(redisKey);

            if (StringUtils.isNotBlank(cachedProfile)) {

                SearchUserApiContent profile =
                        objectMapper.readValue(cachedProfile, SearchUserApiContent.class);

                userProfiles.put(userId, profile);

            } else {
                missedUsers.add(userId);
            }
        }

        if (!missedUsers.isEmpty()) {

            List<String> fields = Arrays.asList(
                    Constants.USER_ID,
                    Constants.FIRST_NAME,
                    "profileDetails");

            Map<String, Object> fetchedUsers =
                    userUtilityService.getUsersDataFromUserIds(
                            missedUsers, fields, token);

            for (Map.Entry<String, Object> entry : fetchedUsers.entrySet()) {

                SearchUserApiContent profile =
                        (SearchUserApiContent) entry.getValue();

                userProfiles.put(entry.getKey(), profile);

                redisCacheMgr.putCache(
                        Constants.PC_USER_PROFILE_CACHE_KEY + entry.getKey(),
                        profile,
                        3600);
            }
        }

        return userProfiles;
    }
}
