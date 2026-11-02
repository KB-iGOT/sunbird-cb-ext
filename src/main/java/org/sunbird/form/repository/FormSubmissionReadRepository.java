package org.sunbird.form.repository;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.sunbird.common.util.Constants;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reads form submissions from the cb-ext-form-service Postgres table form_submissions.
 * Rows are returned as maps with the same keys the old fs-forms-data-alias-v2 ES documents
 * had (formId, submittedBy, contextId, responses, peerReviews, ...), so report code that
 * consumed ES hits works unchanged. Postgres keeps one row per (userid, formid, contextid).
 */
@Repository
public class FormSubmissionReadRepository {

    private static final Logger LOGGER = LoggerFactory.getLogger(FormSubmissionReadRepository.class);

    private static final String COLUMNS = "submissionid, formid, userid, contextid, contexttype, contextname, "
            + "contextorgid, status, fullname, submittedby, submitteddate, updatedby, updateddate, version, "
            + "responses, attachments, peerreviews, submissionmeta";

    private static final String USER_SUBMISSION_SQL = "SELECT " + COLUMNS + " FROM form_submissions "
            + "WHERE formid = :formId AND userid = :userId AND contextid = :contextId";

    // Keyset paging on (submitteddate, submissionid): oldest first, so a caller that keys
    // results by user ends up with the latest submission.
    private static final String SUBMISSIONS_PAGE_SQL = "SELECT " + COLUMNS + " FROM form_submissions "
            + "WHERE formid = :formId "
            + "AND (:contextType IS NULL OR contexttype = :contextType) "
            + "AND (:status IS NULL OR UPPER(status) = :status) "
            + "AND (COALESCE(submitteddate, 0), submissionid) > (:afterDate, :afterId) "
            + "ORDER BY COALESCE(submitteddate, 0), submissionid "
            + "LIMIT :limit";

    // ES document keys with no matching entry in Constants
    static final String SUBMISSION_ID = "submissionId";
    private static final String VERSION = "version";

    private static final TypeReference<Object> JSON_TYPE = new TypeReference<Object>() {};

    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public FormSubmissionReadRepository(@Qualifier("formJdbcTemplate") NamedParameterJdbcTemplate jdbcTemplate,
                                        ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * The user's submission for a form in a context (at most one row). A blank contextId
     * matches submissions saved without a context, stored as ''.
     */
    public List<Map<String, Object>> findUserSubmission(String formId, String userId, String contextId) {
        MapSqlParameterSource params = new MapSqlParameterSource()
                .addValue("formId", formId)
                .addValue("userId", userId)
                .addValue("contextId", StringUtils.defaultString(contextId));
        return jdbcTemplate.query(USER_SUBMISSION_SQL, params, (rs, rowNum) -> toSubmission(rs));
    }

    /**
     * Streams every submission of a form to the consumer, page by page, oldest first.
     * contextType and status are optional filters (null = any); status is matched case-insensitively.
     */
    public void forEachSubmission(String formId, String contextType, String status, int pageSize,
                                  Consumer<Map<String, Object>> consumer) {
        long afterDate = -1L;
        String afterId = "";
        int total = 0;
        List<Map<String, Object>> page;
        do {
            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("formId", formId)
                    .addValue("contextType", contextType, Types.VARCHAR)
                    .addValue("status", status == null ? null : status.toUpperCase(), Types.VARCHAR)
                    .addValue("afterDate", afterDate)
                    .addValue("afterId", afterId)
                    .addValue("limit", pageSize);
            page = jdbcTemplate.query(SUBMISSIONS_PAGE_SQL, params, (rs, rowNum) -> toSubmission(rs));
            for (Map<String, Object> submission : page) {
                consumer.accept(submission);
            }
            if (!page.isEmpty()) {
                Map<String, Object> last = page.get(page.size() - 1);
                Long lastDate = (Long) last.get(Constants.SUBMITTED_DATE);
                afterDate = lastDate == null ? 0L : lastDate;
                afterId = (String) last.get(SUBMISSION_ID);
                total += page.size();
            }
        } while (page.size() == pageSize);
        LOGGER.info("FormSubmissionReadRepository:: forEachSubmission: formId: {}, contextType: {}, status: {}, rows: {}",
                formId, contextType, status, total);
    }

    Map<String, Object> toSubmission(ResultSet rs) throws SQLException {
        Map<String, Object> submission = new LinkedHashMap<>();
        submission.put(SUBMISSION_ID, rs.getString("submissionid"));
        submission.put(Constants.FORM_ID, rs.getString("formid"));
        submission.put(Constants.SUBMITTED_BY, rs.getString("userid"));
        submission.put(Constants.CONTEXT_ID_CAMEL, rs.getString("contextid"));
        submission.put(Constants.CONTEXT_TYPE, rs.getString("contexttype"));
        submission.put("contextName", rs.getString("contextname"));
        submission.put("contextOrgId", rs.getString("contextorgid"));
        submission.put(Constants.STATUS, rs.getString("status"));
        submission.put(Constants.USER_FULL_NAME, rs.getString("fullname"));
        submission.put(Constants.SUBMITTED_DATE, getLong(rs, "submitteddate"));
        submission.put("updatedBy", rs.getString("updatedby"));
        submission.put("updatedDate", getLong(rs, "updateddate"));
        submission.put(VERSION, getLong(rs, "version"));
        submission.put(Constants.RESPONSES, readJson(rs, "responses"));
        submission.put(Constants.ATTACHMENTS, readJson(rs, "attachments"));
        submission.put(Constants.PEER_REVIEWS, readJson(rs, "peerreviews"));
        submission.put("submissionMeta", readJson(rs, "submissionmeta"));
        return submission;
    }

    private Long getLong(ResultSet rs, String column) throws SQLException {
        long value = rs.getLong(column);
        return rs.wasNull() ? null : value;
    }

    private Object readJson(ResultSet rs, String column) throws SQLException {
        String json = rs.getString(column);
        if (StringUtils.isBlank(json)) {
            return null;
        }
        try {
            return objectMapper.readValue(json, JSON_TYPE);
        } catch (IOException e) {
            LOGGER.error("FormSubmissionReadRepository:: readJson: invalid {} for submissionid: {}",
                    column, rs.getString("submissionid"), e);
            return null;
        }
    }
}
