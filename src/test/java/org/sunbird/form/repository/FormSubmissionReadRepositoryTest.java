package org.sunbird.form.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import com.fasterxml.jackson.databind.ObjectMapper;

class FormSubmissionReadRepositoryTest {

    @Mock
    private NamedParameterJdbcTemplate jdbcTemplate;

    private FormSubmissionReadRepository repository;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
        repository = new FormSubmissionReadRepository(jdbcTemplate, new ObjectMapper());
    }

    @Test
    void toSubmissionUsesEsDocumentKeys() throws Exception {
        ResultSet rs = mock(ResultSet.class);
        when(rs.getString("submissionid")).thenReturn("sub-1");
        when(rs.getString("formid")).thenReturn("form-1");
        when(rs.getString("userid")).thenReturn("user-1");
        when(rs.getString("contextid")).thenReturn("do_1");
        when(rs.getString("contexttype")).thenReturn("peerValidationSurvey");
        when(rs.getString("status")).thenReturn("SUBMITTED");
        when(rs.getString("fullname")).thenReturn("Learner");
        when(rs.getLong("submitteddate")).thenReturn(1793357912257L);
        when(rs.getString("responses")).thenReturn("[{\"questionId\":\"q1\",\"question\":\"Q1\",\"answer\":\"A1\"}]");
        when(rs.getString("peerreviews")).thenReturn("[{\"peerId\":\"peer-1\"}]");

        Map<String, Object> submission = repository.toSubmission(rs);

        assertEquals("sub-1", submission.get("submissionId"));
        assertEquals("form-1", submission.get("formId"));
        assertEquals("user-1", submission.get("submittedBy"));
        assertEquals("do_1", submission.get("contextId"));
        assertEquals("peerValidationSurvey", submission.get("contextType"));
        assertEquals("SUBMITTED", submission.get("status"));
        assertEquals("Learner", submission.get("fullName"));
        assertEquals(1793357912257L, submission.get("submittedDate"));
        assertEquals(Collections.singletonList(map("questionId", "q1", "question", "Q1", "answer", "A1")),
                submission.get("responses"));
        assertEquals(Collections.singletonList(map("peerId", "peer-1")), submission.get("peerReviews"));
        assertNull(submission.get("attachments"));
    }

    @Test
    void forEachSubmissionPagesFromTheLastRowOfEachPage() {
        List<Map<String, Object>> page1 = Arrays.asList(row("sub-a", 10L), row("sub-b", 20L));
        List<Map<String, Object>> page2 = Collections.singletonList(row("sub-c", 30L));
        doReturn(page1, page2).when(jdbcTemplate)
                .query(anyString(), any(SqlParameterSource.class), any(RowMapper.class));

        List<Object> seen = new ArrayList<>();
        repository.forEachSubmission("form-1", "peerValidationSurvey", "submitted", 2,
                submission -> seen.add(submission.get("submissionId")));

        assertEquals(Arrays.asList("sub-a", "sub-b", "sub-c"), seen);
        ArgumentCaptor<SqlParameterSource> params = ArgumentCaptor.forClass(SqlParameterSource.class);
        verify(jdbcTemplate, times(2)).query(anyString(), params.capture(), any(RowMapper.class));

        MapSqlParameterSource first = (MapSqlParameterSource) params.getAllValues().get(0);
        assertEquals(-1L, first.getValue("afterDate"));
        assertEquals("", first.getValue("afterId"));
        assertEquals("SUBMITTED", first.getValue("status"));
        assertEquals("peerValidationSurvey", first.getValue("contextType"));
        assertEquals(2, first.getValue("limit"));

        MapSqlParameterSource second = (MapSqlParameterSource) params.getAllValues().get(1);
        assertEquals(20L, second.getValue("afterDate"));
        assertEquals("sub-b", second.getValue("afterId"));
    }

    @Test
    void forEachSubmissionWithoutFiltersPassesNulls() {
        doReturn(Collections.emptyList()).when(jdbcTemplate)
                .query(anyString(), any(SqlParameterSource.class), any(RowMapper.class));

        repository.forEachSubmission("form-1", null, null, 500, submission -> { });

        ArgumentCaptor<SqlParameterSource> params = ArgumentCaptor.forClass(SqlParameterSource.class);
        verify(jdbcTemplate).query(anyString(), params.capture(), any(RowMapper.class));
        assertNull(params.getValue().getValue("contextType"));
        assertNull(params.getValue().getValue("status"));
    }

    @Test
    void findUserSubmissionTreatsMissingContextAsEmpty() {
        doReturn(Collections.emptyList()).when(jdbcTemplate)
                .query(anyString(), any(SqlParameterSource.class), any(RowMapper.class));

        repository.findUserSubmission("form-1", "user-1", null);

        ArgumentCaptor<SqlParameterSource> params = ArgumentCaptor.forClass(SqlParameterSource.class);
        verify(jdbcTemplate).query(anyString(), params.capture(), any(RowMapper.class));
        assertEquals("form-1", params.getValue().getValue("formId"));
        assertEquals("user-1", params.getValue().getValue("userId"));
        assertEquals("", params.getValue().getValue("contextId"));
    }

    private Map<String, Object> row(String submissionId, Long submittedDate) {
        Map<String, Object> row = new HashMap<>();
        row.put("submissionId", submissionId);
        row.put("submittedDate", submittedDate);
        return row;
    }

    private Map<String, Object> map(String... keyValues) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            map.put(keyValues[i], keyValues[i + 1]);
        }
        return map;
    }
}
