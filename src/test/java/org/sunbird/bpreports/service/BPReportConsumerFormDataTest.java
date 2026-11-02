package org.sunbird.bpreports.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

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
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.form.repository.FormSubmissionReadRepository;

/**
 * Survey and profile-survey reads of the BP report, now served from form_submissions.
 */
class BPReportConsumerFormDataTest {

    @Mock
    private FormSubmissionReadRepository formSubmissionReadRepository;

    @InjectMocks
    private BPReportConsumer consumer;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void userFormDataComesFromTheRepository() {
        List<Map<String, Object>> rows = Collections.singletonList(submission("user-1", answer("Q1", "A1")));
        when(formSubmissionReadRepository.findUserSubmission("survey-1", "user-1", "do_1")).thenReturn(rows);

        assertEquals(rows, ReflectionTestUtils.invokeMethod(consumer, "getUserFormData", "survey-1", "user-1", "do_1"));
    }

    @Test
    void userFormDataIsEmptyWhenNothingFoundOrTheReadFails() {
        when(formSubmissionReadRepository.findUserSubmission("survey-1", "user-1", "do_1"))
                .thenReturn(Collections.emptyList());
        when(formSubmissionReadRepository.findUserSubmission("survey-1", "user-2", "do_1"))
                .thenThrow(new DataRetrievalFailureException("db down"));

        List<?> none = ReflectionTestUtils.invokeMethod(consumer, "getUserFormData", "survey-1", "user-1", "do_1");
        List<?> failed = ReflectionTestUtils.invokeMethod(consumer, "getUserFormData", "survey-1", "user-2", "do_1");

        assertTrue(none.isEmpty());
        assertTrue(failed.isEmpty());
    }

    @Test
    void batchFormDataKeysAnswersByUserAndKeepsTheLatest() {
        Map<String, Object> repeatedQuestion = submission("user-1", null);
        repeatedQuestion.put("responses", Arrays.asList(answer("Group", "Group B"), answer("Group", "Group C")));
        feed(submission("user-1", answer("Group", "Group A")),
                submission("user-2", null),
                repeatedQuestion);

        Map<String, Object> result = ReflectionTestUtils.invokeMethod(consumer, "getUserBatchFormData", "profile-1");

        assertEquals(1, result.size());
        assertEquals(Collections.singletonMap("Group", "Group C"), result.get("user-1"));
    }

    @Test
    void batchFormDataIsEmptyWhenTheReadFails() {
        doThrow(new DataRetrievalFailureException("db down")).when(formSubmissionReadRepository)
                .forEachSubmission(eq("profile-1"), isNull(), isNull(), anyInt(), any());

        Map<String, Object> result = ReflectionTestUtils.invokeMethod(consumer, "getUserBatchFormData", "profile-1");

        assertTrue(result.isEmpty());
    }

    @SafeVarargs
    private final void feed(Map<String, Object>... submissions) {
        doAnswer(invocation -> {
            Consumer<Map<String, Object>> callback = invocation.getArgument(4);
            Arrays.stream(submissions).forEach(callback);
            return null;
        }).when(formSubmissionReadRepository).forEachSubmission(eq("profile-1"), isNull(), isNull(), anyInt(), any());
    }

    private static Map<String, Object> submission(String userId, Map<String, Object> answer) {
        Map<String, Object> submission = new HashMap<>();
        submission.put("submittedBy", userId);
        submission.put("responses", answer == null ? null : Collections.singletonList(answer));
        return submission;
    }

    private static Map<String, Object> answer(String question, Object value) {
        Map<String, Object> answer = new HashMap<>();
        answer.put("question", question);
        answer.put("answer", value);
        return answer;
    }
}
