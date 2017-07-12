package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import java.io.IOException;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ErrorUtilsTest extends HekateTestBase {
    @Test
    public void testIsCausedBy() {
        assertFalse(ErrorUtils.isCausedBy(Exception.class, null));
        assertFalse(ErrorUtils.isCausedBy(IOException.class, new Exception()));

        assertTrue(ErrorUtils.isCausedBy(IOException.class, new IOException()));
        assertTrue(ErrorUtils.isCausedBy(IOException.class, new Exception(new IOException())));
        assertTrue(ErrorUtils.isCausedBy(IOException.class, new Exception(new Exception(new IOException()))));
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
