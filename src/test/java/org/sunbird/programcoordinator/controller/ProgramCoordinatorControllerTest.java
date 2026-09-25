package org.sunbird.programcoordinator.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.programcoordinator.dto.ProgramCoordinatorUpsertRequest;
import org.sunbird.programcoordinator.service.ProgramCoordinatorService;

public class ProgramCoordinatorControllerTest {

    private static final String TOKEN = "auth-token";
    private static final String PROGRAM_ID = "program-1";

    @Mock
    private ProgramCoordinatorService programCoordinatorService;

    @InjectMocks
    private ProgramCoordinatorController controller;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    private SBApiResponse responseWith(HttpStatus status) {
        SBApiResponse response = new SBApiResponse();
        response.setResponseCode(status);
        return response;
    }

    @Test
    public void upsertCoordinator_delegatesRequestBodyWithIsCoTrainerFlag() {
        ProgramCoordinatorUpsertRequest request = new ProgramCoordinatorUpsertRequest();
        request.setUserId(UUID.randomUUID());
        request.setRoleId((short) 10);
        request.setStatus((short) 1);
        request.setIsCoTrainer(true);
        List<ProgramCoordinatorUpsertRequest> requestBody = Collections.singletonList(request);

        SBApiResponse expected = responseWith(HttpStatus.OK);
        when(programCoordinatorService.upsert(PROGRAM_ID, requestBody, TOKEN)).thenReturn(expected);

        ResponseEntity<?> entity = controller.upsertCoordinator(PROGRAM_ID, TOKEN, requestBody);

        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(programCoordinatorService).upsert(PROGRAM_ID, requestBody, TOKEN);
    }

    @Test
    public void listCoordinators_delegatesAndPropagatesStatus() {
        Map<String, Object> requestBody = new HashMap<>();
        SBApiResponse expected = responseWith(HttpStatus.OK);
        when(programCoordinatorService.list(eq(PROGRAM_ID), eq(requestBody), eq(TOKEN))).thenReturn(expected);

        ResponseEntity<?> entity = controller.listCoordinators(PROGRAM_ID, requestBody, TOKEN);

        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(programCoordinatorService).list(PROGRAM_ID, requestBody, TOKEN);
    }

    @Test
    public void getProgramCoordinators_delegatesAndPropagatesStatus() {
        SBApiResponse expected = responseWith(HttpStatus.OK);
        when(programCoordinatorService.getProgramCoordinator(PROGRAM_ID, TOKEN)).thenReturn(expected);

        ResponseEntity<SBApiResponse> entity = controller.getProgramCoordinators(PROGRAM_ID, TOKEN);

        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(programCoordinatorService).getProgramCoordinator(PROGRAM_ID, TOKEN);
    }

    @Test
    public void getCoordinatorRoles_delegatesAndPropagatesStatus() {
        SBApiResponse expected = responseWith(HttpStatus.OK);
        when(programCoordinatorService.getCoordinatorRoles(TOKEN)).thenReturn(expected);

        ResponseEntity<SBApiResponse> entity = controller.getCoordinatorRoles(TOKEN);

        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(programCoordinatorService).getCoordinatorRoles(TOKEN);
    }

    @Test
    public void upsertByAdmin_delegatesAndPropagatesStatus() {
        List<ProgramCoordinatorUpsertRequest> requestBody = Collections.emptyList();
        SBApiResponse expected = responseWith(HttpStatus.OK);
        when(programCoordinatorService.upsertByAdmin(PROGRAM_ID, requestBody, TOKEN)).thenReturn(expected);

        ResponseEntity<SBApiResponse> entity = controller.upsertByAdmin(PROGRAM_ID, requestBody, TOKEN);

        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(programCoordinatorService).upsertByAdmin(PROGRAM_ID, requestBody, TOKEN);
    }
}
