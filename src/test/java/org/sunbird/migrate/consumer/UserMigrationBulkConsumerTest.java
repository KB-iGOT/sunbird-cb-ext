package org.sunbird.migrate.consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.migrate.service.OrgHierarchyServiceClient;
import org.sunbird.migrate.service.impl.UserMigrationServiceImpl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserMigrationBulkConsumerTest {

    private static final String ROOT_ORG_ID = "root-org-1";
    private static final String ACCESS_TOKEN = "test-access-token";
    private static final String FRAMEWORK_ID = "root-org-1_org_hierarchy";

    private OrgHierarchyServiceClient orgHierarchyServiceClient;
    private CbExtServerProperties serverProperties;
    private UserMigrationServiceImpl userMigrationService;
    private UserMigrationBulkConsumer consumer;

    @BeforeEach
    void setUp() {
        orgHierarchyServiceClient = mock(OrgHierarchyServiceClient.class);
        serverProperties = mock(CbExtServerProperties.class);
        userMigrationService = mock(UserMigrationServiceImpl.class);
        consumer = new UserMigrationBulkConsumer(orgHierarchyServiceClient);
        ReflectionTestUtils.setField(consumer, "serverProperties", serverProperties);
        ReflectionTestUtils.setField(consumer, "userMigrationService", userMigrationService);
    }

    @Test
    @SuppressWarnings("unchecked")
    void resolveOrgHierarchyContext_cbOrgHierarchyServiceEnabled_buildsDescendantMapAndChannelMap() {
        when(serverProperties.isBulkUserTransferUseCbOrgHierarchyService()).thenReturn(true);

        Map<String, String> rootOrg = new HashMap<>();
        rootOrg.put("id", ROOT_ORG_ID);
        rootOrg.put("channel", "Root Org");
        Map<String, String> childOrg = new HashMap<>();
        childOrg.put("id", "child-org-1");
        childOrg.put("channel", "Child Org");
        when(orgHierarchyServiceClient.fetchMasterData(anyString(), anyString()))
                .thenReturn(Arrays.asList(rootOrg, childOrg));

        Map.Entry<Map<String, Set<String>>, Map<String, String>> context = ReflectionTestUtils.invokeMethod(
                consumer, "resolveOrgHierarchyContext", null, ROOT_ORG_ID, ACCESS_TOKEN);

        Map<String, Set<String>> orgHierarchyMap = context.getKey();
        Map<String, String> orgIdToChannelMap = context.getValue();

        assertTrue(orgHierarchyMap.get(ROOT_ORG_ID).contains("child-org-1"));
        assertFalse(orgHierarchyMap.get(ROOT_ORG_ID).contains(ROOT_ORG_ID));
        assertEquals("Root Org", orgIdToChannelMap.get(ROOT_ORG_ID));
        assertEquals("Child Org", orgIdToChannelMap.get("child-org-1"));
    }

    @Test
    void resolveOrgHierarchyContext_cbOrgHierarchyServiceDisabled_fallsBackToFrameworkUtilData() throws Exception {
        when(serverProperties.isBulkUserTransferUseCbOrgHierarchyService()).thenReturn(false);
        when(userMigrationService.getCompleteFrameworkDataFromUtil(FRAMEWORK_ID)).thenReturn(new HashMap<>());

        Map.Entry<Map<String, Set<String>>, Map<String, String>> context = ReflectionTestUtils.invokeMethod(
                consumer, "resolveOrgHierarchyContext", FRAMEWORK_ID, ROOT_ORG_ID, ACCESS_TOKEN);

        assertTrue(context.getKey().isEmpty());
        assertTrue(context.getValue().isEmpty());
    }
}
