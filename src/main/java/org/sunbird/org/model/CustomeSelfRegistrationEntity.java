package org.sunbird.org.model;

import lombok.*;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

/**
 * @author mahesh.vakkund
 */

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@IdClass(CustomeSelfRegistrationId.class)  // Specify the composite ID class
@Table(name = "registration_qr_code")
@Entity
public class CustomeSelfRegistrationEntity {

    @Id
    @Column(name = "orgid")
    @NotNull
    private String orgId;

    @Id
    @Column(name = "id")
    @NotNull
    private String id;

    @Column(name = "status")
    private String status;

    @Column(name = "url")
    private String url;

    @Column(name = "startdate")
    private String startDate;

    @Column(name = "enddate")
    private String endDate;

    @Column(name = "createdby")
    private String createdBy;

    @Column(name = "createddatetime")
    private String createdDateTime;

    @Column(name = "numberofusersonboarded")
    private Long numberOfUsersOnboarded;

    @Column(name = "qrcodeimagepath")
    private String qrCodeImagePath;

}
