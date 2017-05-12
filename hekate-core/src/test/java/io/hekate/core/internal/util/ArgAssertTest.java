package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import org.junit.Test;

public class ArgAssertTest extends HekateTestBase {
    public static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(ArgAssert.class);
    }

    @Test
    public void testCheck() throws Exception {
        expectErrorMessage(IAE, "test message", () ->
            ArgAssert.check(false, "test message")
        );

        ArgAssert.check(true, "no error");
    }

    @Test
    public void testNotNull() throws Exception {
        expectErrorMessage(IAE, "something must be not null.", () ->
            ArgAssert.notNull(null, "something")
        );

        ArgAssert.notNull(new Object(), "no error");
    }

    @Test
    public void testNotEmpty() throws Exception {
        expectErrorMessage(IAE, "something must be not null.", () ->
            ArgAssert.notEmpty(null, "something")
        );

        expectErrorMessage(IAE, "something must be have non-whitespace characters.", () ->
            ArgAssert.notEmpty("", "something")
        );

        expectErrorMessage(IAE, "something must be have non-whitespace characters.", () ->
            ArgAssert.notEmpty("   ", "something")
        );

        expectErrorMessage(IAE, "something must be have non-whitespace characters.", () ->
            ArgAssert.notEmpty("\n", "something")
        );

        expectErrorMessage(IAE, "something must be have non-whitespace characters.", () ->
            ArgAssert.notEmpty(System.lineSeparator(), "something")
        );

        ArgAssert.notEmpty("not empty", "no error");
        ArgAssert.notEmpty("   not empty\n", "no error");
    }

    @Test
    public void testIsFalse() throws Exception {
        expectErrorMessage(IAE, "test message", () ->
            ArgAssert.isFalse(true, "test message")
        );

        ArgAssert.isFalse(false, "no error");
    }

    @Test
    public void testIsTrue() throws Exception {
        expectErrorMessage(IAE, "test message", () ->
            ArgAssert.isTrue(false, "test message")
        );

        ArgAssert.isTrue(true, "no error");
    }
}
