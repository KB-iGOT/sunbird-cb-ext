package org.sunbird.profile.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.core.producer.Producer;

/**
 * KPI 1.4: VERIFIED_PROFILE karma event published from profileMDOAdminUpdate once the
 * profile moves to VERIFIED and the LMS update succeeds.
 */
class ProfileServiceImplTest {

    private static final String KARMA_TOPIC = "dev.karma.points.unified.v2.event";
    private static final String USER_ID = "user-123";

    private ProfileServiceImpl profileService;
    private Producer kafkaProducer;

    @BeforeEach
    void setUp() {
        profileService = new ProfileServiceImpl();
        kafkaProducer = mock(Producer.class);
        CbExtServerProperties serverConfig = mock(CbExtServerProperties.class);
        when(serverConfig.getKarmaPointsUnifiedEventTopic()).thenReturn(KARMA_TOPIC);
        ReflectionTestUtils.setField(profileService, "kafkaProducer", kafkaProducer);
        ReflectionTestUtils.setField(profileService, "serverConfig", serverConfig);
    }

    @Test
    @SuppressWarnings("unchecked")
    void raiseVerifiedProfileKarmaEvent_publishesEventKeyedByUserId() {
        ReflectionTestUtils.invokeMethod(profileService, "raiseVerifiedProfileKarmaEventIfEligible", USER_ID);

        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer, times(1)).pushWithKey(eq(KARMA_TOPIC), eventCaptor.capture(), eq(USER_ID));

        Map<String, Object> event = (Map<String, Object>) eventCaptor.getValue();
        assertEquals(Constants.EVENT_TYPE_VERIFIED_PROFILE, event.get(Constants.EVENT_TYPE));
        assertEquals(Constants.KARMA_POINTS_EVENT_VERSION, event.get(Constants.KAFKA_EVENT_VERSION_KEY));
        Map<String, Object> data = (Map<String, Object>) event.get(Constants.DATA);
        Map<String, Object> edata = (Map<String, Object>) data.get(Constants.EDATA);
        assertEquals(USER_ID, edata.get(Constants.USER_ID));
        assertEquals(1, edata.size());
    }

    @Test
    void raiseVerifiedProfileKarmaEvent_producerFailure_isNotPropagated() {
        doThrow(new RuntimeException("kafka down")).when(kafkaProducer).pushWithKey(anyString(), any(), anyString());

        assertDoesNotThrow(() ->
                ReflectionTestUtils.invokeMethod(profileService, "raiseVerifiedProfileKarmaEventIfEligible", USER_ID));
        verify(kafkaProducer, times(1)).pushWithKey(eq(KARMA_TOPIC), any(), eq(USER_ID));
    }

    @Test
    void verifiedProfileEventContractConstants() {
        assertEquals("VERIFIED_PROFILE", Constants.EVENT_TYPE_VERIFIED_PROFILE);
        assertEquals(1, Constants.KARMA_POINTS_EVENT_VERSION);
        assertEquals("edata", Constants.EDATA);
        assertEquals("eventType", Constants.EVENT_TYPE);
        assertEquals("version", Constants.KAFKA_EVENT_VERSION_KEY);
    }
}
