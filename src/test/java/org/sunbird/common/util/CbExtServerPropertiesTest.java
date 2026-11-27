package org.sunbird.common.util;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CbExtServerPropertiesTest {

    @Test
    void orgHierarchyServiceProperties_returnConfiguredValues() {
        CbExtServerProperties serverConfig = new CbExtServerProperties();
        ReflectionTestUtils.setField(serverConfig, "orgHierarchyServiceUrl", "http://localhost:8888");
        ReflectionTestUtils.setField(serverConfig, "orgHierarchyServiceCompletePath", "/org/v2/hierarchy");
        ReflectionTestUtils.setField(serverConfig, "orgHierarchyServiceMasterDataRedisTtl", 14400);
        ReflectionTestUtils.setField(serverConfig, "bulkUserTransferUseCbOrgHierarchyService", true);

        assertEquals("http://localhost:8888", serverConfig.getOrgHierarchyServiceUrl());
        assertEquals("/org/v2/hierarchy", serverConfig.getOrgHierarchyServiceCompletePath());
        assertEquals(14400, serverConfig.getOrgHierarchyServiceMasterDataRedisTtl());
        assertTrue(serverConfig.isBulkUserTransferUseCbOrgHierarchyService());
    }
}
