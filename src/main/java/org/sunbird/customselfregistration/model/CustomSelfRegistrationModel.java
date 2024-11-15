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
    Long registrationstartdate;
    Long registrationenddate;
    String logo;
    String description;
    String orgId;
    String registrationLink;
    String qrCodeFilePath;
}
