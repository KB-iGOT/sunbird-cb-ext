package org.sunbird.smtsltupgrade.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.smtsltupgrade.service.SmtToSltUpgradeService;

class SmtToSltUpgradeControllerTest {

    @Test
    void shouldReturnServiceResponseWithItsStatusCode() {
        SmtToSltUpgradeService upgradeService = mock(SmtToSltUpgradeService.class);
        SBApiResponse serviceResponse = new SBApiResponse("api.smt.slt.upgrade.run");
        serviceResponse.setResponseCode(HttpStatus.OK);
        when(upgradeService.runSmtToSltUpgradeCheck()).thenReturn(serviceResponse);

        ResponseEntity<SBApiResponse> result = new SmtToSltUpgradeController(upgradeService).run();

        assertEquals(HttpStatus.OK, result.getStatusCode());
        assertSame(serviceResponse, result.getBody());
        verify(upgradeService).runSmtToSltUpgradeCheck();
    }

    @Test
    void shouldPropagateInternalServerErrorFromService() {
        SmtToSltUpgradeService upgradeService = mock(SmtToSltUpgradeService.class);
        SBApiResponse serviceResponse = new SBApiResponse("api.smt.slt.upgrade.run");
        serviceResponse.setResponseCode(HttpStatus.INTERNAL_SERVER_ERROR);
        when(upgradeService.runSmtToSltUpgradeCheck()).thenReturn(serviceResponse);

        ResponseEntity<SBApiResponse> result = new SmtToSltUpgradeController(upgradeService).run();

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.getStatusCode());
    }
}
