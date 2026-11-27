package org.sunbird.migrate.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.migrate.service.OrgHierarchyServiceClient;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UserMigrationServiceImplTest {

    private static final String FRAMEWORK_ID = "root-org-1_org_hierarchy";
    private static final String ROOT_ORG_ID = "root-org-1";
    private static final String TOKEN_ORG_ID = "sub-org-1";
    private static final String ACCESS_TOKEN = "test-access-token";

    private OrgHierarchyServiceClient orgHierarchyServiceClient;
    private CbExtServerProperties serverConfig;
    private UserMigrationServiceImpl service;

    @BeforeEach
    void setUp() {
        orgHierarchyServiceClient = mock(OrgHierarchyServiceClient.class);
        serverConfig = mock(CbExtServerProperties.class);
        service = new UserMigrationServiceImpl(orgHierarchyServiceClient);
        ReflectionTestUtils.setField(service, "serverConfig", serverConfig);
    }

    @Test
    void getOrgList_cbOrgHierarchyServiceEnabled_rootAdmin_usesBaseFrameworkOrgId() {
        when(serverConfig.isBulkUserTransferUseCbOrgHierarchyService()).thenReturn(true);
        List<Map<String, String>> masterData = Collections.singletonList(Collections.singletonMap("id", ROOT_ORG_ID));
        when(orgHierarchyServiceClient.fetchMasterData(anyString(), anyString())).thenReturn(masterData);

        List<Map<String, String>> result = ReflectionTestUtils.invokeMethod(service, "getOrgList",
                FRAMEWORK_ID, ROOT_ORG_ID, ACCESS_TOKEN);

        assertEquals(masterData, result);
        verify(orgHierarchyServiceClient).fetchMasterData(ROOT_ORG_ID, ACCESS_TOKEN);
    }

    @Test
    void getOrgList_cbOrgHierarchyServiceEnabled_nonRootAdmin_usesTokenOrgId() {
        when(serverConfig.isBulkUserTransferUseCbOrgHierarchyService()).thenReturn(true);
        List<Map<String, String>> masterData = Collections.singletonList(Collections.singletonMap("id", TOKEN_ORG_ID));
        when(orgHierarchyServiceClient.fetchMasterData(anyString(), anyString())).thenReturn(masterData);

        List<Map<String, String>> result = ReflectionTestUtils.invokeMethod(service, "getOrgList",
                FRAMEWORK_ID, TOKEN_ORG_ID, ACCESS_TOKEN);

        assertEquals(masterData, result);
        verify(orgHierarchyServiceClient).fetchMasterData(TOKEN_ORG_ID, ACCESS_TOKEN);
    }
}
