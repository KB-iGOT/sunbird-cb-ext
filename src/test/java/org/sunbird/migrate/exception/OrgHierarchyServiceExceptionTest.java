package org.sunbird.migrate.exception;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class OrgHierarchyServiceExceptionTest {

    @Test
    void constructor_setsMessageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        OrgHierarchyServiceException exception = new OrgHierarchyServiceException("failed to parse", cause);

        assertEquals("failed to parse", exception.getMessage());
        assertSame(cause, exception.getCause());
    }
}
