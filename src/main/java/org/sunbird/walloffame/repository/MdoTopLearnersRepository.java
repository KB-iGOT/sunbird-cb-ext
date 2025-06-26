package org.sunbird.walloffame.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.sunbird.walloffame.entity.MdoTopLearnersEntity;

import java.util.List;

public interface MdoTopLearnersRepository extends JpaRepository<MdoTopLearnersEntity, String> {
    List<MdoTopLearnersEntity> findByOrgIdAndRowNumIn(String orgId, List<Integer> rowNum);
}
