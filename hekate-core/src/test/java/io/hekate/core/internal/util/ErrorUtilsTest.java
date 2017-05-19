package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ErrorUtilsTest extends HekateTestBase {
    @Test
    public void testIsCausedBy() {
        assertFalse(ErrorUtils.isCausedBy(null, Exception.class));
        assertFalse(ErrorUtils.isCausedBy(new Exception(), IOException.class));

        assertTrue(ErrorUtils.isCausedBy(new IOException(), IOException.class));
        assertTrue(ErrorUtils.isCausedBy(new Exception(new IOException()), IOException.class));
        assertTrue(ErrorUtils.isCausedBy(new Exception(new Exception(new IOException())), IOException.class));
    }

    @Test
    public void testStackTrace() {
        assertTrue(ErrorUtils.stackTrace(new Exception()).contains(ErrorUtilsTest.class.getName()));
        assertTrue(ErrorUtils.stackTrace(new Exception()).contains(ErrorUtilsTest.class.getName() + ".testStackTrace("));
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(ErrorUtils.class);
    }
}
