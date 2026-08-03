package org.sunbird.programcoordinator.consumer;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
            List<UUID> uuidList = userIds.stream().map(UUID::fromString).collect(Collectors.toList());
            List<UserProgramProjection> records = programCoordinatorRepository.findActiveProgramsByUserIds(uuidList);

            Map<UUID, List<String>> userProgramMap = records.stream()
                    .collect(Collectors.groupingBy(
                            UserProgramProjection::getUserId,
                            Collectors.mapping(UserProgramProjection::getProgramId, Collectors.toList())));

            BulkRequest bulkRequest = new BulkRequest();
            for (String userId : userIds) {
                try {
                    List<String> dbProgramIds = userProgramMap.getOrDefault(
                            UUID.fromString(userId), Collections.emptyList());

                    Set<String> mergedProgramIds = new HashSet<>(dbProgramIds);
                    Map<String, Object> existingDoc = fetchExistingDoc(userId);
                    if (existingDoc != null) {
                        List<String> existingIds = (List<String>) existingDoc.get("programIds");
                        if (existingIds != null) {
                            mergedProgramIds.addAll(existingIds);
                        }
                    }

                    Map<String, Object> document = new HashMap<>();
                    document.put("userId", userId);
                    document.put("programIds", new ArrayList<>(mergedProgramIds));
                    document.put("updatedOn", Instant.now().toString());

                    bulkRequest.add(new IndexRequest(userProgramLookupIndex, "_doc")
                            .id(userId).source(document));
                } catch (Exception e) {
                    log.error("Error preparing lookup document for user {}", userId, e);
                }
            }

            if (bulkRequest.numberOfActions() > 0) {
                BulkResponse response = sbEsClient.bulk(bulkRequest, RequestOptions.DEFAULT);

                if (response.hasFailures()) {
                    log.error("Bulk sync failures: {}", response.buildFailureMessage());
                } else {
                    log.info("Successfully synced {} documents", bulkRequest.numberOfActions());
                }
            }

        } catch (Exception e) {
            log.error("Error syncing lookup index", e);
        }
    }

    private Map<String, Object> fetchExistingDoc(String userId) {
        try {
            GetRequest getRequest = new GetRequest(userProgramLookupIndex, "_doc", userId);
            GetResponse response = sbEsClient.get(getRequest, RequestOptions.DEFAULT);
            return response.isExists() ? response.getSourceAsMap() : null;
        } catch (Exception e) {
            log.warn("Could not fetch existing doc for user {}", userId, e);
            return null;
        }
    }

}
