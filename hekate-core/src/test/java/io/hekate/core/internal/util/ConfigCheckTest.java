/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.core.internal.util;

import io.hekate.HekateTestBase;
import io.hekate.core.HekateConfigurationException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import org.junit.Test;

public class ConfigCheckTest extends HekateTestBase {
    private static final String PREFIX = "ConfigCheckTest: ";

    private static final Class<HekateConfigurationException> HCE = HekateConfigurationException.class;

    private final ConfigCheck check = ConfigCheck.get(ConfigCheckTest.class);

    @Test
    public void testThatSuccess() {
        check.that(true, "Success");
    }

    @Test
    public void testThatFailure() {
        expectExactMessage(HCE, PREFIX + "Test message", () ->
            check.that(Boolean.valueOf("false"), "Test message")
        );

        check.that(true, "Success");
    }

    @Test
    public void testFail() {
        expectExactMessage(HCE, PREFIX + "java.lang.Exception: test error message", () ->
            check.fail(new Exception("test error message"))
        );
    }

    @Test
    public void testIsPowerOfTwo() {
        expectExactMessage(HCE, PREFIX + "Ten must be a power of two [value=10]", () ->
            check.isPowerOfTwo(10, "Ten")
        );

        check.isPowerOfTwo(8, "Success");
    }

    @Test
    public void testRange() {
        expectExactMessage(HCE, PREFIX + "Ten must be within the 1..5 range.", () ->
            check.range(10, 1, 5, "Ten")
        );

        check.range(0, 0, 0, "0-0-0");
        check.range(1, 0, 1, "1-0-1");
        check.range(1, 1, 1, "1-1-1");
        check.range(5, 1, 8, "5-1-8");
    }

    @Test
    public void testPositiveInt() {
        expectExactMessage(HCE, PREFIX + "Zero must be greater than 0 [value=0]", () ->
            check.positive(0, "Zero")
        );

        expectExactMessage(HCE, PREFIX + "Negative must be greater than 0 [value=-1]", () ->
            check.positive(-1, "Negative")
        );

        check.positive(1, "Success");
    }

    @Test
    public void testPositiveLong() {
        expectExactMessage(HCE, PREFIX + "Zero must be greater than 0 [value=0]", () ->
            check.positive(0L, "Zero")
        );

        expectExactMessage(HCE, PREFIX + "Negative must be greater than 0 [value=-1]", () ->
            check.positive(-1L, "Negative")
        );

        check.positive(1L, "Success");
    }

    @Test
    public void testNonNegativeInt() {
        expectExactMessage(HCE, PREFIX + "Negative must be greater than or equals to 0 [value=-1]", () ->
            check.nonNegative(-1, "Negative")
        );

        check.nonNegative(0, "Success");
        check.nonNegative(1, "Success");
    }

    @Test
    public void testUnique() {
        expectExactMessage(HCE, PREFIX + "duplicated One [value=one]", () ->
            check.unique("one", Collections.singleton("one"), "One")
        );

        check.unique("one", Collections.emptySet(), "Success");
        check.unique("one", Collections.singleton("two"), "Success");
        check.unique("one", new HashSet<>(Arrays.asList("two", "three")), "Success");
    }

    @Test
    public void testIsTrue() {
        expectExactMessage(HCE, PREFIX + "True Epic fail", () ->
            check.isTrue(Boolean.valueOf("false"), "True Epic fail")
        );

        check.isTrue(true, "Success");
    }

    @Test
    public void testIsFalse() {
        check.isFalse(false, "Success");

        expectExactMessage(HCE, PREFIX + "Epic fail", () ->
            check.isFalse(true, "Epic fail")
        );
    }

    @Test
    public void testNotNull() {
        expectExactMessage(HCE, PREFIX + "Epic fail must be not null.", () ->
            check.notNull(null, "Epic fail")
        );

        check.notNull("777", "Success");
    }

    @Test
    public void testNotEmpty() {
        expectExactMessage(HCE, PREFIX + "Epic fail must be not null.", () ->
            check.notEmpty(null, "Epic fail")
        );

        expectExactMessage(HCE, PREFIX + "Epic fail must be a non-empty string.", () ->
            check.notEmpty("", "Epic fail")
        );

        expectExactMessage(HCE, PREFIX + "Epic fail must be a non-empty string.", () ->
            check.notEmpty("  ", "Epic fail")
        );

        expectExactMessage(HCE, PREFIX + "Epic fail must be a non-empty string.", () ->
            check.notEmpty("\n", "Epic fail")
        );

        check.notEmpty("777", "Success");
    }

    @Test
    public void testSysName() {
        expectExactMessage(HCE, PREFIX + "Epic fail can contain only alpha-numeric characters and non-repeatable dots/hyphens "
                + "[value=-not-valid-]",
            () -> check.validSysName("-not-valid-", "Epic fail")
        );

        check.validSysName(null, "ignore");
        check.validSysName("", "ignore");
        check.validSysName("  ", "ignore");
        check.validSysName("X", "ignore");
        check.validSysName("XXX", "ignore");
        check.validSysName("x", "ignore");
        check.validSysName("xxx", "ignore");
        check.validSysName("xxx-x", "ignore");
        check.validSysName("xxx-xxx", "ignore");
        check.validSysName("x-xxx", "ignore");
        check.validSysName("xxx.x", "ignore");
        check.validSysName("xxx.xxx", "ignore");
        check.validSysName("x.xxx", "ignore");
        check.validSysName("x.xxx-x", "ignore");
        check.validSysName("x-xxx.x", "ignore");
        check.validSysName("x-xxx.x-x.x.x", "ignore");
        check.validSysName("x-xxx.x-x.x.x", "ignore");
        check.validSysName("  x-xxx.x-x.x.x  ", "ignore");

        expect(HCE, () -> check.validSysName("-", "ignore"));
        expect(HCE, () -> check.validSysName(".", "ignore"));
        expect(HCE, () -> check.validSysName("x..x", "ignore"));
        expect(HCE, () -> check.validSysName("x--x", "ignore"));
        expect(HCE, () -> check.validSysName("-xxx", "ignore"));
        expect(HCE, () -> check.validSysName("xxx-", "ignore"));
        expect(HCE, () -> check.validSysName(".xxx-", "ignore"));
        expect(HCE, () -> check.validSysName("xxx.", "ignore"));
        expect(HCE, () -> check.validSysName("xxx-.-xxx", "ignore"));
    }
}
