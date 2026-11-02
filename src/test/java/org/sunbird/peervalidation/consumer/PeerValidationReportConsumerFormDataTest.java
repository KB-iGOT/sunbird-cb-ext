package org.sunbird.peervalidation.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.IndexerService;
import org.sunbird.form.repository.FormSubmissionReadRepository;

/**
 * Peer validation report rows, now read from form_submissions.
 */
class PeerValidationReportConsumerFormDataTest {

    @Mock
    private FormSubmissionReadRepository formSubmissionReadRepository;

    @Mock
    private IndexerService indexerService;

    @Mock
    private CbExtServerProperties serverProperties;

    @InjectMocks
    private PeerValidationReportConsumer consumer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        consumer.setFormSubmissionReadRepository(formSubmissionReadRepository);
    }

    @Test
    void csvHasOneRowPerSubmittedSubmission() throws Exception {
        Map<String, Object> withPeers = submission("user-1", "Learner One",
                Collections.singletonList(Collections.singletonMap(Constants.PEER_ID, "peer-1")));
        Map<String, Object> withoutPeers = submission("user-2", "Learner Two", null);
        doAnswer(invocation -> {
            Consumer<Map<String, Object>> callback = invocation.getArgument(4);
            callback.accept(withPeers);
            callback.accept(withoutPeers);
            return null;
        }).when(formSubmissionReadRepository).forEachSubmission(eq("form-1"),
                eq(Constants.CONTEXT_TYPE_PEER_VALIDATION_SURVEY), eq(Constants.SUBMITTED), anyInt(), any());

        Map<String, Object> question = new HashMap<>();
        question.put(Constants.SUBMISSION_QUESTION_ID, "q1");
        question.put(Constants.NAME, "Q1");
        question.put(Constants.ORDER, 1);
        File csv = ReflectionTestUtils.invokeMethod(consumer, "generateCSVReport", "form-1", "do_1",
                Collections.singletonList(question));

        try {
            List<String> lines = Files.readAllLines(csv.toPath(), StandardCharsets.UTF_8);
            assertEquals(3, lines.size()); // header + 2 submissions
            assertTrue(lines.get(1).startsWith("Learner One"));
            assertTrue(lines.get(2).startsWith("Learner Two"));
        } finally {
            Files.deleteIfExists(csv.toPath());
        }
    }

    private static Map<String, Object> submission(String userId, String fullName, List<Map<String, Object>> peers) {
        Map<String, Object> submission = new HashMap<>();
        submission.put(Constants.SUBMITTED_BY, userId);
        submission.put(Constants.USER_FULL_NAME, fullName);
        submission.put(Constants.SUBMITTED_DATE, 1773590091000L);
        submission.put(Constants.SUBMISSION_RESPONSES, Arrays.asList(
                answer("q1", "A1")));
        submission.put(Constants.PEER_REVIEWS, peers);
        return submission;
    }

    private static Map<String, Object> answer(String questionId, String value) {
        Map<String, Object> answer = new HashMap<>();
        answer.put(Constants.SUBMISSION_QUESTION_ID, questionId);
        answer.put(Constants.ANSWER, value);
        return answer;
    }
}
