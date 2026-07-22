package org.sunbird.nongovtuser.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Data holder for user row fields during validation.
 * Addresses SonarQube issue of too many parameters in validateRow method.
 */
@Getter
@AllArgsConstructor
public class UserRowData {
    private final String fullName;
    private final String mobileNumber;
    private final String email;
    private final String externalId;
    private final String gender;
    private final String category;
    private final String motherTongue;
    private final String dob;
    private final String officePincode;
}
