package org.sunbird.programcoordinator.service;

/**
 * Async phase of the Program Coordinator bulk-upload flow, invoked from
 * ProgramCoordinatorBulkUploadConsumer for each Kafka event published by
 * ProgramCoordinatorBulkUploadServiceImpl.
 */
public interface ProgramCoordinatorBulkUploadProcessingService {

    /**
     * Parses the Kafka payload, downloads and processes the uploaded file, and persists the
     * final outcome. Never throws - any failure is caught, logged and reflected in the
     * tracking record's status.
     *
     * @param inputData JSON-serialized flat string map published by
     *                  ProgramCoordinatorBulkUploadServiceImpl.triggerBulkUploadKafkaEvent
     */
    void initiateProgramCoordinatorBulkUploadProcess(String inputData);
}
