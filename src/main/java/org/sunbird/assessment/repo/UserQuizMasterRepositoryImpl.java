package org.sunbird.assessment.repo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.CassandraBatchOperations;
import org.springframework.data.cassandra.core.CassandraOperations;

public class UserQuizMasterRepositoryImpl implements UserQuizMasterRepositoryCustom {

	@Autowired
	@Qualifier("cassandraTemplate")
	CassandraOperations cassandraOperations;
	
	@Override
	public UserQuizMasterModel updateQuiz(UserQuizMasterModel quiz, UserQuizSummaryModel quizSummary) {
		CassandraBatchOperations batchOps = cassandraOperations.batchOps();
		batchOps.insert(quiz);
		batchOps.insert(quizSummary);
		batchOps.execute();
		return quiz;
	}
}
