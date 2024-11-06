package org.sunbird.bpreports.postgres.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.sunbird.bpreports.postgres.entity.WfStatusEntity;

public interface WfStatusEntityRepository extends JpaRepository<WfStatusEntity, String> {

    Page<WfStatusEntity> findByApplicationId(String applicationId, Pageable pageable);
}
