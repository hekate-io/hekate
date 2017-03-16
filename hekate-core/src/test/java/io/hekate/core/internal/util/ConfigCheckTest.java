package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateConfigurationException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

public class ConfigCheckTest extends HekateTestBase {
    public static final String PREFIX = "ConfigCheckTest: ";

    private final ConfigCheck check = ConfigCheck.get(ConfigCheckTest.class);

    @Test
    public void testThatSuccess() {
        check.that(true, "Success");
    }

    @Test
    public void testThatFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Test message", () ->
            check.that(Boolean.valueOf("false"), "Test message")
        );
    }

    @Test
    public void testRangeSuccess() {
        check.range(0, 0, 0, "0-0-0");
        check.range(1, 0, 1, "1-0-1");
        check.range(1, 1, 1, "1-1-1");
        check.range(5, 1, 8, "5-1-8");
    }

    @Test
    public void testRangeFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Ten must be within the 1..5 range.", () ->
            check.range(10, 1, 5, "Ten")
        );
    }

    @Test
    public void testPositiveIntSuccess() {
        check.positive(1, "Success");
    }

    @Test
    public void testPositiveLongSuccess() {
        check.positive(1L, "Success");
    }

    @Test
    public void testPositiveIntFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Zero must be greater than 0 [value=0]", () ->
            check.positive(0, "Zero")
        );

        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Negative must be greater than 0 [value=-1]", () ->
            check.positive(-1, "Negative")
        );
    }

    @Test
    public void testNonNegativeIntSuccess() {
        check.nonNegative(0, "Success");
        check.nonNegative(1, "Success");
    }

    @Test
    public void testNonNegativeIntFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Negative must be greater than or equals to 0 [value=-1]",
            () -> check.nonNegative(-1, "Negative")
        );
    }

    @Test
    public void testUniqueSuccess() {
        check.unique("one", Collections.emptySet(), "Success");
        check.unique("one", Collections.singleton("two"), "Success");
        check.unique("one", new HashSet<>(Arrays.asList("two", "three")), "Success");
    }

    @Test
    public void testUniqueFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "duplicated One [value=one]",
            () -> check.unique("one", Collections.singleton("one"), "One")
        );
    }

    @Test
    public void testIsTrueSuccess() {
        check.isTrue(true, "Success");
    }

    @Test
    public void testIsTrueFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "True Epic fail",
            () -> check.isTrue(Boolean.valueOf("false"), "True Epic fail")
        );
    }

    @Test
    public void testIsFalseSuccess() {
        check.isFalse(false, "Success");
    }

    @Test
    public void testIsFalseFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Epic fail",
            () -> check.isFalse(true, "Epic fail")
        );
    }

    @Test
    public void testNotNullSuccess() {
        check.notNull("777", "Success");
    }

    @Test
    public void testNotNullFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Epic fail must be not null.",
            () -> check.notNull(null, "Epic fail")
        );
    }

    @Test
    public void testNotEmptySuccess() {
        check.notEmpty("777", "Success");
    }

    @Test
    public void testNotEmptyFailure() {
        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Epic fail must be not null.",
            () -> check.notEmpty(null, "Epic fail")
        );

        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Epic fail must be a non-empty string.",
            () -> check.notEmpty("", "Epic fail")
        );

        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Epic fail must be a non-empty string.",
            () -> check.notEmpty("  ", "Epic fail")
        );

        expectErrorMessage(HekateConfigurationException.class, PREFIX + "Epic fail must be a non-empty string.",
            () -> check.notEmpty("\n", "Epic fail")
        );
    }
}
