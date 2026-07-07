package org.sunbird.nongovtuser.service;

/**
 * Async processing side of Non-Govt (Volunteer) bulk user upload.
 * Consumer counterpart to the sync {@link NonGovtUserBulkUploadService}. Mirrors
 * UserBulkUploadService (the govt equivalent) up through receiving/validating the
 * Kafka event and marking the job IN-PROGRESS. Per-row CSV processing (file download,
 * field validation, user creation) is a separate, not-yet-built layer.
 */
public interface NonGovtUserBulkUploadProcessingService {

    /**
     * Entry point invoked by {@link NonGovtUserBulkUploadConsumer} for each Kafka message.
     *
     * @param inputData JSON payload published by NonGovtUserBulkUploadServiceImpl.triggerBulkUploadKafkaEvent
     */
    void initiateNonGovtUserBulkUploadProcess(String inputData);
}
