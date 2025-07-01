package org.sunbird.insights.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.sunbird.insights.entity.LearnerStatsEntity;

public interface LearnerStatsRepository extends JpaRepository<LearnerStatsEntity, String> {

}
