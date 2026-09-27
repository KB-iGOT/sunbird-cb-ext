package org.sunbird.programcoordinator.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import org.sunbird.common.util.Constants;

/**
 * Aggregates per-row outcomes from ProgramCoordinatorBulkUploadProcessingServiceImpl's
 * CSV/XLSX row processing into overall counts plus the rows to write back out in the
 * results file. Scoped-down equivalent of nongovtuser.model.RowProcessingSummary, kept
 * local to this package rather than reused across feature packages.
 */
@Getter
@Setter
public class ProgramCoordinatorBulkUploadRowSummary {

    private int totalRecords;
    private int successfulRecords;
    private int failedRecords;
    private final List<Map<String, String>> updatedRecords = new ArrayList<>();

    public void recordRow(Map<String, String> row) {
        updatedRecords.add(new HashMap<>(row));
        totalRecords++;
        if (Constants.SUCCESSFUL_UPPERCASE.equalsIgnoreCase(row.get(Constants.PASCALCASESTATUS))) {
            successfulRecords++;
        } else {
            failedRecords++;
        }
    }

    public List<Map<String, String>> getUpdatedRecords() {
        return Collections.unmodifiableList(updatedRecords);
    }
}
