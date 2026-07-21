package org.sunbird.nongovtuser.model;

import lombok.Builder;
import lombok.Getter;

/**
 * DTO for CSV/XLSX row data extraction to reduce method parameters.
 * Addresses SonarQube issue of too many parameters in extractRowFields.
 */
@Getter
@Builder
public class CsvRowData {
    private final String fullName;
    private final String mobileNumber;
    private final String email;
    private final String externalId;
    private final String gender;
    private final String category;
    private final String dob;
    private final String motherTongue;
    private final String officePincode;
    private final String tags;
    private final String orgName;
}
