package org.sunbird.customselfregistration.model;

import lombok.*;

/**
 * @author mahesh.vakkund
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CustomSelfRegistrationModel {
    String registrationstartdate;
    String registrationenddate;
    String logo;
    String description;
    String orgId;
    String registrationLink;
    String qrCodeFilePath;
    String id;
    String status;
    String createdby;
    String createddatetime;
    Long numberofusersonboarded;
    String qrLogoFilePath;
}
