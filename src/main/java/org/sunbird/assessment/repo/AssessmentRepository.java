package org.sunbird.assessment.repo;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.sunbird.assessment.dto.AssessmentSubmissionDTO;
import org.sunbird.common.model.SBApiResponse;
import org.sunbird.common.util.Constants;
import org.sunbird.cqfassessment.model.CQFAssessmentModel;

public interface AssessmentRepository {

	/**
	 * gets answer key for the assessment given the url
	 *
	 * @param artifactUrl
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> getAssessmentAnswerKey(String artifactUrl) throws Exception;

	/**
	 * gets answerkey for the quiz submission
	 *
	 * @param quizMap
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> getQuizAnswerKey(AssessmentSubmissionDTO quizMap) throws Exception;

	/**
	 * inserts quiz or assessments for a user
	 *
	 * @param persist
	 * @param isAssessment
	 * @return
	 * @throws Exception
	 */
	public Map<String, Object> insertQuizOrAssessment(Map<String, Object> persist, Boolean isAssessment)
			throws Exception;

	/**
	 * gets assessment for a user given a content id
	 *
	 * @param courseId
	 * @param userId
	 * @return
	 * @throws Exception
	 */
	public List<Map<String, Object>> getAssessmentbyContentUser(String rootOrg, String courseId, String userId)
			throws Exception;

	List<Map<String, Object>> fetchUserAssessmentDataFromDB(String userId, String assessmentIdentifier);

	boolean addUserAssesmentDataToDB(String userId, String assessmentId, Timestamp startTime, Timestamp endTime,
									 Map<String, Object> questionSet, String status);

	Boolean updateUserAssesmentDataToDB(String userId, String assessmentIdentifier,
										Map<String, Object> submitAssessmentRequest, Map<String, Object> submitAssessmentResponse, String status,
										Date startTime,Map<String, Object> saveSubmitAssessmentRequest);

	/**
	 * Adds the user's CQF assessment data to the database.
	 *
	 * @param cqfAssessmentModel The CQFAssessmentModel object representing the assessment.
	 * @param startTime          The timestamp marking the start of the assessment.
	 * @param endTime            The timestamp marking the end of the assessment.
	 * @param questionSet        The map containing the question set data.
	 * @param status             The status of the assessment.
	 * @return True if the assessment data was added successfully, false otherwise.
	 */
	boolean addUserCQFAssesmentDataToDB(CQFAssessmentModel cqfAssessmentModel, Timestamp startTime,
										Timestamp endTime, Map<String, Object> questionSet, String status);
}