package org.sunbird.walloffame.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import org.sunbird.walloffame.entity.UserLeaderboardEntity;

@Repository
public interface  UserLeaderboardRepository extends JpaRepository<UserLeaderboardEntity, String> {

}
