package org.sunbird.programcoordinator.consumer;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.sunbird.programcoordinator.entity.UserProgramProjection;
import org.sunbird.programcoordinator.repository.ProgramCoordinatorRepository;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class ProgramCoordinatorSyncService {

    private final RestHighLevelClient sbEsClient;
    private final ProgramCoordinatorRepository programCoordinatorRepository;

    @Value("${user.program.lookup.index:user_program_lookup_v1}")
    private String userProgramLookupIndex;

    public ProgramCoordinatorSyncService(
            @Qualifier("sbEsClient") RestHighLevelClient sbEsClient,
            ProgramCoordinatorRepository programCoordinatorRepository) {
        this.sbEsClient = sbEsClient;
        this.programCoordinatorRepository = programCoordinatorRepository;
    }

    public void syncUserProgramLookup(List<String> userIds) {

        try {

            List<UUID> uuidList = userIds.stream()
                    .map(UUID::fromString)
                    .collect(Collectors.toList());

            List<UserProgramProjection> records =
                    programCoordinatorRepository.findActiveProgramsByUserIds(uuidList);

            Map<UUID, List<String>> userProgramMap =
                    records.stream()
                            .collect(Collectors.groupingBy(
                                    UserProgramProjection::getUserId,
                                    Collectors.mapping(
                                            UserProgramProjection::getProgramId,
                                            Collectors.toList())));

            BulkRequest bulkRequest = new BulkRequest();

            for (String userId : userIds) {

                try {

                    List<String> programIds = userProgramMap.getOrDefault(
                            UUID.fromString(userId),
                            Collections.emptyList());

                    Map<String, Object> document = new HashMap<>();
                    document.put("userId", userId);
                    document.put("programIds", programIds);
                    document.put("updatedOn", Instant.now().toString());

                    bulkRequest.add(
                            new IndexRequest(userProgramLookupIndex, "_doc")
                                    .id(userId)
                                    .source(document)
                    );

                } catch (Exception e) {
                    log.error("Error preparing lookup document for user {}", userId, e);
                }
            }

            if (bulkRequest.numberOfActions() == 0) {
                return;
            }

            BulkResponse response = sbEsClient.bulk(bulkRequest, RequestOptions.DEFAULT);

            if (response.hasFailures()) {
                log.error("Bulk indexing completed with failures: {}", response.buildFailureMessage());
            } else {
                log.info("Successfully synced lookup index for {} users", bulkRequest.numberOfActions());
            }

        } catch (Exception e) {
            log.error("Error syncing lookup index", e);
        }
    }

}
