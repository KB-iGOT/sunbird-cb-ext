package org.sunbird.programcoordinator.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Role master (program_coordinator_role). A handful of rows, effectively static reference data.
 * No CRUD endpoints are exposed for this table — it exists so program_coordinator can carry a
 * role_id FK and so the list API can join in role_name.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "program_coordinator_role")
@Entity
public class ProgramCoordinatorRoleEntity {

    @Id
    @Column(name = "id")
    private Short id;

    @Column(name = "role_code")
    private String roleCode;

    @Column(name = "role_name")
    private String roleName;

    @Column(name = "is_active")
    private Boolean isActive;
}
