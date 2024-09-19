package org.sunbird.org.model;

public class Result {
    private NestedResult result;
    private long totalCount;

    public NestedResult getResult() {
        return result;
    }

    public void setResult(NestedResult result) {
        this.result = result;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(long totalCount) {
        this.totalCount = totalCount;
    }
}
