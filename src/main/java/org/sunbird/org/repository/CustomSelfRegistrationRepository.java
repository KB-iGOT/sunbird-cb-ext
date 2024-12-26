package org.sunbird.org.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.sunbird.org.model.CustomeSelfRegistrationEntity;

import javax.transaction.Transactional;
import java.util.List;

/**
 * @author mahesh.vakkund
 */

public interface CustomSelfRegistrationRepository extends JpaRepository<CustomeSelfRegistrationEntity, String> {
    @Query(value = "SELECT * from registration_qr_code where orgid=?1", nativeQuery = true)
    List<CustomeSelfRegistrationEntity> findAllByOrgId(String orgId);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = "UPDATE registration_qr_code SET status = ?3 WHERE orgid = ?1 AND id = ?2", nativeQuery = true)
    void updateRegistrationQrCodeWithStatus(String orgId, String uniqueId, String status);

    @Query(value = "SELECT * from registration_qr_code where orgid=?1 and id=?2", nativeQuery = true)
    CustomeSelfRegistrationEntity findAllByOrgIdAndUniqueId(String orgId,String uniqueId);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query(value = "UPDATE registration_qr_code SET numberofusersonboarded = ?3 WHERE orgid = ?1 AND id = ?2", nativeQuery = true)
    void updateRegistrationQrCodeOnboardUserCount(String orgId, String uniqueId, long status);

    @Query(value = "SELECT * from registration_qr_code where id=?1", nativeQuery = true)
    CustomeSelfRegistrationEntity findAllByUniqueId(String uniqueId);
}
