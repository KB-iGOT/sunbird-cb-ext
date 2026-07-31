package org.sunbird.programcoordinator.entity;

import java.sql.Timestamp;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Membership row: which user holds which coordinator role on which programme.
 * Holds no name/email — those are resolved from Cassandra/Elasticsearch by the caller.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@IdClass(ProgramCoordinatorId.class)
@Table(name = "program_coordinator")
@Entity
public class ProgramCoordinatorEntity {

    @Id
    @Column(name = "program_id")
    @NotNull
    private String programId;

    @Id
    @Column(name = "user_id")
    @NotNull
    private UUID userId;

    @Column(name = "role_id")
    private Short roleId;

    @Column(name = "status")
    private Short status;

    @Column(name = "created_by")
    private UUID createdBy;

    @Column(name = "created_on")
    private Timestamp createdOn;

    @Column(name = "updated_by")
    private UUID updatedBy;

    @Column(name = "updated_on")
    private Timestamp updatedOn;
}
