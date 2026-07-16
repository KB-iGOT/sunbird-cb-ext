package org.sunbird.nongovtuser.model;

import lombok.Builder;
import lombok.Getter;

/**
 * DTO for volunteer user creation request to reduce method parameters
 * and improve code readability. Addresses SonarQube issue of too many parameters.
 *
 * Mandatory fields: fullName, mobileNumber, email
 * Optional fields: externalId, gender, category, dob, motherTongue, officePincode, tags
 */
@Getter
@Builder
public class VolunteerUserRequest {
    private final String fullName;
    private final String mobileNumber;
    private final String email;
    private final String externalId;
    private final String orgName;
    private final String orgId;
    private final String userAuthToken;
    private final int rowIndex;
    private final String gender;
    private final String category;
    private final String dob;
    private final String motherTongue;
    private final String officePincode;
    private final String tags;
}
