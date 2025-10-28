package org.sunbird.orghierarchyreport.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.sunbird.orghierarchyreport.entity.MdoChildrenLookupEntity;

import java.util.List;

public interface MdoChildrenLookUpRepository extends JpaRepository<MdoChildrenLookupEntity, String> {

    @Query(value = "SELECT * from mdo_children_lookup where mdo_id in (?1)", nativeQuery = true)
    List<MdoChildrenLookupEntity> findAllChildrenByMdoId(List<String> sbOrgIdList);
}
