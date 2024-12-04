package org.sunbird.bpreports.postgres.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.sunbird.bpreports.postgres.entity.WfStatusEntity;

import java.util.List;

public interface WfStatusEntityRepository extends JpaRepository<WfStatusEntity, String> {

    Page<WfStatusEntity> findByApplicationId(String applicationId, Pageable pageable);

    @Query(value = "SELECT * FROM wf_status WHERE userid= :userId and service_name= :service and current_status= :currentStatus", nativeQuery = true)
    List<WfStatusEntity> findProfileApprovalRequests(String userId, String service, String currentStatus);
}
