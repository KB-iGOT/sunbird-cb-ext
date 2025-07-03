package org.sunbird.walloffame.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.sunbird.walloffame.entity.SlwMdoLeaderBoardEntity;

import java.util.List;
@Repository
public interface SlwMdoLeaderBoardRepository extends JpaRepository<SlwMdoLeaderBoardEntity, SlwMdoLeaderBoardEntity.SlwMdoLeaderBoardKey> {
    List<SlwMdoLeaderBoardEntity> findByParentIdAndSizeIn(String parentId, List<String> size);
}
