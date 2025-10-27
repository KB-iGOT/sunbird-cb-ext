package org.sunbird.org.warehouse.entity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.LocalDateTime;

@Entity
@Table(name = "mdo_children_lookup" ,schema = "public")
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class MdoChildrenLookupEntity {

    @Id
    @Column(name = "mdo_id")
    private String mdoId;

    @Column(name = "children_id")
    private String childrenId;

    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

}
