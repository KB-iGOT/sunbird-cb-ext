package org.sunbird.programcoordinator.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.cache.RedisCacheMgr;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.AccessTokenValidator;
import org.sunbird.common.util.Constants;
import org.sunbird.core.producer.Producer;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorEntity;
import org.sunbird.programcoordinator.entity.ProgramCoordinatorRoleEntity;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorListDto;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRepository;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRoleRepository;
import org.sunbird.user.service.UserUtilityService;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ProgramCoordinatorServiceImplTest {

    private static final String TOKEN = "valid-token";
    private static final String PROGRAM_ID = "program-1";
    private static final String ALLOWED_ROLE = "PROGRAM_MANAGER";
    private static final Short LEAD_TRAINER_ROLE_ID = 10;
    private static final Short BASE_ROLE_ID = 1;

    @Mock
    private ProgramCoordinatorRepository programCoordinatorRepository;

    @Mock
    private ProgramCoordinatorRoleRepository programCoordinatorRoleRepository;

    @Mock
    private AccessTokenValidator accessTokenValidator;

    @Mock
    private RedisCacheMgr redisCacheMgr;

    @Mock
    private Producer kafkaProducer;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private UserUtilityService userUtilityService;

    @InjectMocks
    private ProgramCoordinatorServiceImpl service;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        ReflectionTestUtils.setField(service, "sortableFields",
                Arrays.asList("createdOn", "updatedOn", "roleId"));
        ReflectionTestUtils.setField(service, "allowedRoles", Collections.singletonList(ALLOWED_ROLE));
        ReflectionTestUtils.setField(service, "adminAllowedRoles", Collections.singletonList(ALLOWED_ROLE));
        ReflectionTestUtils.setField(service, "defaultLimit", 20);
        ReflectionTestUtils.setField(service, "coordinatorSyncTopic", "coordinator-sync-topic");

        ProgramCoordinatorRoleEntity baseRole = ProgramCoordinatorRoleEntity.builder()
                .id(BASE_ROLE_ID)
                .roleCode("PROGRAM_COORDINATOR")
                .roleName(Constants.PROGRAM_COORDINATOR_KEY)
                .isActive(true)
                .build();

        ProgramCoordinatorRoleEntity leadTrainerRole = ProgramCoordinatorRoleEntity.builder()
                .id(LEAD_TRAINER_ROLE_ID)
                .roleCode("NATIONAL_LEAD_TRAINER")
                .roleName("National Lead Trainer")
                .isActive(true)
                .build();

        when(programCoordinatorRoleRepository.findAll())
                .thenReturn(Arrays.asList(baseRole, leadTrainerRole));

        service.loadRoles();

        mockAuthorizedActor();
    }

    private void mockAuthorizedActor() {
        when(accessTokenValidator.fetchUserRolesFromToken(TOKEN))
                .thenReturn(Collections.singletonList(ALLOWED_ROLE));
        when(accessTokenValidator.fetchUserIdFromAccessToken(TOKEN))
                .thenReturn(UUID.randomUUID().toString());
    }

    private Map<String, Object> listRequestBody() {
        Map<String, Object> innerRequest = new HashMap<>();
        innerRequest.put(Constants.LIMIT, 20);
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put(Constants.REQUEST, innerRequest);
        return requestBody;
    }

    private ProgramCoordinatorUpsertRequest activeRequest(UUID userId, Short roleId, Boolean isCoTrainer) {
        ProgramCoordinatorUpsertRequest request = new ProgramCoordinatorUpsertRequest();
        request.setUserId(userId);
        request.setRoleId(roleId);
        request.setStatus(Constants.ACTIVE_STATUS_PC);
        request.setIsCoTrainer(isCoTrainer);
        return request;
    }

    @Test
    public void upsert_storesIsCoTrainerTrueWhenProvided() {
        UUID userId = UUID.randomUUID();
        when(programCoordinatorRoleRepository.existsById(LEAD_TRAINER_ROLE_ID)).thenReturn(true);
        when(programCoordinatorRepository.addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(LEAD_TRAINER_ROLE_ID), eq(Boolean.TRUE), any(UUID.class)))
                .thenReturn(1);

        SBApiResponse response = service.upsert(PROGRAM_ID,
                Collections.singletonList(activeRequest(userId, LEAD_TRAINER_ROLE_ID, true)), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(programCoordinatorRepository).addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(LEAD_TRAINER_ROLE_ID), eq(Boolean.TRUE), any(UUID.class));

        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESPONSE);
        assertTrue(((List<String>) result.get(Constants.ADDED_OR_UPDATED)).contains(userId.toString()));
    }

    @Test
    public void upsert_defaultsIsCoTrainerToFalseWhenOmitted() {
        UUID userId = UUID.randomUUID();
        when(programCoordinatorRoleRepository.existsById(LEAD_TRAINER_ROLE_ID)).thenReturn(true);
        when(programCoordinatorRepository.addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(LEAD_TRAINER_ROLE_ID), eq(Boolean.FALSE), any(UUID.class)))
                .thenReturn(1);

        SBApiResponse response = service.upsert(PROGRAM_ID,
                Collections.singletonList(activeRequest(userId, LEAD_TRAINER_ROLE_ID, null)), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(programCoordinatorRepository).addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(LEAD_TRAINER_ROLE_ID), eq(Boolean.FALSE), any(UUID.class));
    }

    @Test
    public void upsert_softRemove_neverTouchesIsCoTrainer() {
        UUID userId = UUID.randomUUID();
        ProgramCoordinatorUpsertRequest request = new ProgramCoordinatorUpsertRequest();
        request.setUserId(userId);
        request.setStatus(Constants.INACTIVE_STATUS_PC);

        when(programCoordinatorRepository.softRemove(eq(PROGRAM_ID), eq(userId), any(UUID.class)))
                .thenReturn(1);

        SBApiResponse response = service.upsert(PROGRAM_ID, Collections.singletonList(request), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(programCoordinatorRepository, never()).addOrResurrect(
                anyString(), any(UUID.class), anyShort(), any(), any(UUID.class));
    }

    @Test
    public void upsert_forbiddenWhenActorLacksAllowedRole() {
        when(accessTokenValidator.fetchUserRolesFromToken(TOKEN))
                .thenReturn(Collections.singletonList("SOME_OTHER_ROLE"));

        SBApiResponse response = service.upsert(PROGRAM_ID,
                Collections.singletonList(activeRequest(UUID.randomUUID(), LEAD_TRAINER_ROLE_ID, true)), TOKEN);

        assertEquals(HttpStatus.FORBIDDEN, response.getResponseCode());
        verify(programCoordinatorRepository, never()).addOrResurrect(
                anyString(), any(UUID.class), anyShort(), any(), any(UUID.class));
    }

    @Test
    public void upsertByAdmin_defaultsIsCoTrainerToFalseAndRoleToDefaultWhenOmitted() {
        UUID userId = UUID.randomUUID();
        ProgramCoordinatorUpsertRequest request = new ProgramCoordinatorUpsertRequest();
        request.setUserId(userId);
        request.setStatus(Constants.ACTIVE_STATUS_PC);

        when(programCoordinatorRepository.addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(BASE_ROLE_ID), eq(Boolean.FALSE), any(UUID.class)))
                .thenReturn(1);

        SBApiResponse response = service.upsertByAdmin(PROGRAM_ID, Collections.singletonList(request), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(programCoordinatorRepository).addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(BASE_ROLE_ID), eq(Boolean.FALSE), any(UUID.class));
    }

    @Test
    public void upsertByAdmin_passesThroughExplicitIsCoTrainerTrue() {
        UUID userId = UUID.randomUUID();
        ProgramCoordinatorUpsertRequest request = activeRequest(userId, LEAD_TRAINER_ROLE_ID, true);

        when(programCoordinatorRoleRepository.existsById(LEAD_TRAINER_ROLE_ID)).thenReturn(true);
        when(programCoordinatorRepository.addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(LEAD_TRAINER_ROLE_ID), eq(Boolean.TRUE), any(UUID.class)))
                .thenReturn(1);

        SBApiResponse response = service.upsertByAdmin(PROGRAM_ID, Collections.singletonList(request), TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        verify(programCoordinatorRepository).addOrResurrect(
                eq(PROGRAM_ID), eq(userId), eq(LEAD_TRAINER_ROLE_ID), eq(Boolean.TRUE), any(UUID.class));
    }

    @Test
    public void list_mapsIsCoTrainerFlagForEachCoordinator() {
        UUID coTrainerUser = UUID.randomUUID();
        UUID nonCoTrainerUser = UUID.randomUUID();

        ProgramCoordinatorListDto coTrainer = new ProgramCoordinatorListDto(
                coTrainerUser, LEAD_TRAINER_ROLE_ID, "National Lead Trainer", true,
                UUID.randomUUID(), null, null);
        ProgramCoordinatorListDto nonCoTrainer = new ProgramCoordinatorListDto(
                nonCoTrainerUser, LEAD_TRAINER_ROLE_ID, "National Lead Trainer", false,
                UUID.randomUUID(), null, null);

        Page<ProgramCoordinatorListDto> page = new PageImpl<>(Arrays.asList(coTrainer, nonCoTrainer));

        when(programCoordinatorRepository.findCoordinators(eq(PROGRAM_ID), any(Pageable.class)))
                .thenReturn(page);
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(userUtilityService.getUsersDataFromUserIds(anyList(), anyList(), eq(TOKEN)))
                .thenReturn(Collections.emptyMap());

        Map<String, Object> requestBody = listRequestBody();

        SBApiResponse response = service.list(PROGRAM_ID, requestBody, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESPONSE);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.get(Constants.CONTENT);

        Map<String, Object> coTrainerEntry = content.stream()
                .filter(m -> coTrainerUser.equals(m.get(Constants.USER_ID)))
                .findFirst().orElseThrow(() -> new AssertionError("co-trainer entry missing"));
        Map<String, Object> nonCoTrainerEntry = content.stream()
                .filter(m -> nonCoTrainerUser.equals(m.get(Constants.USER_ID)))
                .findFirst().orElseThrow(() -> new AssertionError("non-co-trainer entry missing"));

        assertEquals(Boolean.TRUE, coTrainerEntry.get(Constants.IS_CO_TRAINER));
        assertEquals(Boolean.FALSE, nonCoTrainerEntry.get(Constants.IS_CO_TRAINER));
    }

    @Test
    public void list_defaultsIsCoTrainerToFalseWhenNullInDto() {
        UUID userId = UUID.randomUUID();

        ProgramCoordinatorListDto dtoWithNullFlag = new ProgramCoordinatorListDto(
                userId, LEAD_TRAINER_ROLE_ID, "National Lead Trainer", null,
                UUID.randomUUID(), null, null);

        Page<ProgramCoordinatorListDto> page = new PageImpl<>(Collections.singletonList(dtoWithNullFlag));

        when(programCoordinatorRepository.findCoordinators(eq(PROGRAM_ID), any(Pageable.class)))
                .thenReturn(page);
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(userUtilityService.getUsersDataFromUserIds(anyList(), anyList(), eq(TOKEN)))
                .thenReturn(Collections.emptyMap());

        Map<String, Object> requestBody = listRequestBody();

        SBApiResponse response = service.list(PROGRAM_ID, requestBody, TOKEN);

        Map<String, Object> result = (Map<String, Object>) response.get(Constants.RESPONSE);
        List<Map<String, Object>> content = (List<Map<String, Object>>) result.get(Constants.CONTENT);

        assertFalse(content.isEmpty());
        assertEquals(Boolean.FALSE, content.get(0).get(Constants.IS_CO_TRAINER));
    }

    @Test
    public void getProgramCoordinator_mapsIsCoTrainerFromEntity() {
        ProgramCoordinatorEntity coordinator = ProgramCoordinatorEntity.builder()
                .programId(PROGRAM_ID)
                .userId(UUID.randomUUID())
                .roleId(LEAD_TRAINER_ROLE_ID)
                .status(Constants.ACTIVE_STATUS_PC)
                .isCoTrainer(true)
                .build();

        when(programCoordinatorRepository.findActiveByProgramId(PROGRAM_ID))
                .thenReturn(Collections.singletonList(coordinator));
        when(redisCacheMgr.getCache(anyString())).thenReturn(null);
        when(userUtilityService.getUsersDataFromUserIds(anyList(), anyList(), eq(TOKEN)))
                .thenReturn(Collections.emptyMap());

        SBApiResponse response = service.getProgramCoordinator(PROGRAM_ID, TOKEN);

        assertEquals(HttpStatus.OK, response.getResponseCode());
        List<Map<String, Object>> coordinators =
                (List<Map<String, Object>>) response.getResult().get(Constants.COORDINATORS);

        assertEquals(Boolean.TRUE, coordinators.get(0).get(Constants.IS_CO_TRAINER));
    }
}
