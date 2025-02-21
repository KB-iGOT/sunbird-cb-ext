package org.sunbird.common.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SunbirdApiBatchResp implements Serializable {
	private List<String> createdFor;
	private String endDate;
	private String name;
	private String batchId;
	private String enrollmentType;
	private String enrollmentEndDate;
	private String startDate;
	private int status;
	private Map<String, Object> batchAttributes = new HashMap<>();
	public List<String> getCreatedFor() {
		return createdFor;
	}
	public void setCreatedFor(List<String> createdFor) {
		this.createdFor = createdFor;
	}
	public String getEndDate() {
		return endDate;
	}
	public void setEndDate(String endDate) {
		this.endDate = endDate;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getBatchId() {
		return batchId;
	}
	public void setBatchId(String batchId) {
		this.batchId = batchId;
	}
	public String getEnrollmentType() {
		return enrollmentType;
	}
	public void setEnrollmentType(String enrollmentType) {
		this.enrollmentType = enrollmentType;
	}
	public String getEnrollmentEndDate() {
		return enrollmentEndDate;
	}
	public void setEnrollmentEndDate(String enrollmentEndDate) {
		this.enrollmentEndDate = enrollmentEndDate;
	}
	public String getStartDate() {
		return startDate;
	}
	public void setStartDate(String startDate) {
		this.startDate = startDate;
	}
	public int getStatus() {
		return status;
	}
	public void setStatus(int status) {
		this.status = status;
	}

	public Map<String, Object> getBatchAttributes() {
		return batchAttributes;
	}

	public void setBatchAttributes(Map<String, Object> batchAttributes) {
		this.batchAttributes = batchAttributes;
	}
}
