package org.sunbird.walloffame.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.sunbird.walloffame.entity.MdoLeaderboardEntity;

import java.util.List;

@Repository
public interface MdoLeaderboardRepository extends JpaRepository<MdoLeaderboardEntity, String> {
    List<MdoLeaderboardEntity> findBySizeIn(List<String> sizes);

}
