package org.sunbird.karmacoinwallet.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.karmacoinwallet.service.KarmaCoinWalletService;

public class KarmaCoinWalletControllerTest {

    private static final String TOKEN = "auth-token";

    @Mock
    private KarmaCoinWalletService karmaCoinWalletService;

    @InjectMocks
    private KarmaCoinWalletController controller;

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
    public void getWalletSummary_delegatesAndPropagatesStatus() {
        SBApiResponse expected = responseWith(HttpStatus.OK);
        when(karmaCoinWalletService.getWalletSummary(TOKEN)).thenReturn(expected);

        ResponseEntity<SBApiResponse> entity = controller.getWalletSummary(TOKEN);

        assertEquals(HttpStatus.OK, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(karmaCoinWalletService).getWalletSummary(TOKEN);
    }

    @Test
    public void getTransactions_delegatesAndPropagatesStatus() {
        Map<String, Object> body = new HashMap<>();
        SBApiResponse expected = responseWith(HttpStatus.BAD_REQUEST);
        when(karmaCoinWalletService.getTransactions(eq(TOKEN), any())).thenReturn(expected);

        ResponseEntity<SBApiResponse> entity = controller.getTransactions(TOKEN, body);

        assertEquals(HttpStatus.BAD_REQUEST, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(karmaCoinWalletService).getTransactions(TOKEN, body);
    }

    @Test
    public void redeem_delegatesAndPropagatesStatus() {
        Map<String, Object> body = new HashMap<>();
        SBApiResponse expected = responseWith(HttpStatus.ACCEPTED);
        when(karmaCoinWalletService.redeem(eq(TOKEN), any())).thenReturn(expected);

        ResponseEntity<SBApiResponse> entity = controller.redeem(TOKEN, body);

        assertEquals(HttpStatus.ACCEPTED, entity.getStatusCode());
        assertSame(expected, entity.getBody());
        verify(karmaCoinWalletService).redeem(TOKEN, body);
    }
}
