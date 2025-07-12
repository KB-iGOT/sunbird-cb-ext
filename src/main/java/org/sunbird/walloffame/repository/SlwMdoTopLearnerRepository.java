package org.sunbird.walloffame.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.sunbird.walloffame.entity.SlwMdoTopLearnerEntity;

import java.util.List;

public interface SlwMdoTopLearnerRepository extends JpaRepository<SlwMdoTopLearnerEntity, SlwMdoTopLearnerEntity.SlwMdoTopLearnerKey> {
    List<SlwMdoTopLearnerEntity> findByOrgIdAndRowNumIn(String orgId, List<Integer> rowNums);
}
