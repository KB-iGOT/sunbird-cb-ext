package org.sunbird.karmapoints.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import org.sunbird.karmapoints.model.ClaimKarmaPointsRequest;

/**
 * KPI 2.2: ACBP_CLAIM karma event published to the karma points unified topic for
 * karma-points-processor-v2 (ACBPClaimHandler).
 */
class KarmaPointsServiceImplTest {

    private static final String UNIFIED_TOPIC = "dev.karma.points.unified.v2.event";
    private static final String OLD_CLAIM_TOPIC = "dev.user.claim.acbp.karma.points";
    private static final String USER_ID = "user-123";
    private static final String COURSE_ID = "do_course_1";

    private KarmaPointsServiceImpl karmaPointsService;
    private Producer kafkaProducer;
    private CbExtServerProperties serverProperties;

    @BeforeEach
    void setUp() {
        karmaPointsService = new KarmaPointsServiceImpl();
        kafkaProducer = mock(Producer.class);
        serverProperties = mock(CbExtServerProperties.class);
        when(serverProperties.getKarmaPointsUnifiedEventTopic()).thenReturn(UNIFIED_TOPIC);
        when(serverProperties.getKafkaEventEnvelopeVersion()).thenReturn(2);
        ReflectionTestUtils.setField(karmaPointsService, "kafkaProducer", kafkaProducer);
        ReflectionTestUtils.setField(karmaPointsService, "serverProperties", serverProperties);
    }

    private ClaimKarmaPointsRequest claimRequest(String userId, String courseId) {
        ClaimKarmaPointsRequest request = new ClaimKarmaPointsRequest();
        request.setUserId(userId);
        request.setCourseId(courseId);
        return request;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> captureEvent(String key) {
        ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaProducer, times(1)).pushWithKey(eq(UNIFIED_TOPIC), eventCaptor.capture(), eq(key));
        return (Map<String, Object>) eventCaptor.getValue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void claimKarmaPoints_publishesAcbpClaimEventToUnifiedTopicKeyedByUserId() {
        karmaPointsService.claimKarmaPoints(claimRequest(USER_ID, COURSE_ID));

        Map<String, Object> event = captureEvent(USER_ID);
        assertEquals(Constants.EVENT_TYPE_ACBP_CLAIM, event.get(Constants.EVENT_TYPE));
        assertEquals(2, event.get(Constants.KAFKA_EVENT_VERSION_KEY));
        assertEquals(3, event.size());

        Map<String, Object> data = (Map<String, Object>) event.get(Constants.DATA);
        assertEquals(1, data.size());
        Map<String, Object> edata = (Map<String, Object>) data.get(Constants.EDATA);
        assertEquals(USER_ID, edata.get(Constants.USER_ID));
        assertEquals(COURSE_ID, edata.get(Constants.COURSE_ID));
        assertEquals(2, edata.size());
    }

    @Test
    void claimKarmaPoints_noLongerPublishesToOldClaimTopic() {
        karmaPointsService.claimKarmaPoints(claimRequest(USER_ID, COURSE_ID));

        verify(kafkaProducer, never()).push(anyString(), any());
        verify(kafkaProducer, never()).pushWithKey(eq(OLD_CLAIM_TOPIC), any(), anyString());
        assertFalse(event().containsKey("edata"), "edata must be nested under data, not at the top level");
    }

    @Test
    void claimKarmaPoints_usesConfiguredEnvelopeVersion() {
        when(serverProperties.getKafkaEventEnvelopeVersion()).thenReturn(3);

        karmaPointsService.claimKarmaPoints(claimRequest(USER_ID, COURSE_ID));

        assertEquals(3, captureEvent(USER_ID).get(Constants.KAFKA_EVENT_VERSION_KEY));
    }

    @Test
    @SuppressWarnings("unchecked")
    void claimKarmaPoints_nullCourseIdIsPassedThroughForTheJobToValidate() {
        karmaPointsService.claimKarmaPoints(claimRequest(USER_ID, null));

        Map<String, Object> data = (Map<String, Object>) captureEvent(USER_ID).get(Constants.DATA);
        Map<String, Object> edata = (Map<String, Object>) data.get(Constants.EDATA);
        assertNull(edata.get(Constants.COURSE_ID));
    }

    @Test
    void claimKarmaPoints_producerFailureIsPropagatedToCaller() {
        doThrow(new RuntimeException("kafka down")).when(kafkaProducer).pushWithKey(anyString(), any(), anyString());

        ClaimKarmaPointsRequest request = claimRequest(USER_ID, COURSE_ID);
        assertThrows(RuntimeException.class, () -> karmaPointsService.claimKarmaPoints(request));
    }

    @Test
    void acbpClaimEventTypeMatchesKarmaPointsV2Job() {
        assertEquals("ACBP_CLAIM", Constants.EVENT_TYPE_ACBP_CLAIM);
        assertEquals("edata", Constants.EDATA);
        assertEquals("data", Constants.DATA);
        assertEquals("eventType", Constants.EVENT_TYPE);
        assertEquals("version", Constants.KAFKA_EVENT_VERSION_KEY);
    }

    private Map<String, Object> event() {
        return captureEvent(USER_ID);
    }
}
