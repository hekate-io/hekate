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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ArgAssertTest extends HekateTestBase {
    public static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(ArgAssert.class);
    }

    @Test
    public void testCheck() throws Exception {
        expectExactMessage(IAE, "test message", () ->
            ArgAssert.check(false, "test message")
        );

        ArgAssert.check(true, "Success");
    }

    @Test
    public void testNotNull() throws Exception {
        expectExactMessage(IAE, "something must be not null.", () ->
            ArgAssert.notNull(null, "something")
        );

        ArgAssert.notNull(new Object(), "Success");
    }

    @Test
    public void testNotEmpty() throws Exception {
        expectExactMessage(IAE, "something must be not null.", () ->
            ArgAssert.notEmpty(null, "something")
        );

        expectExactMessage(IAE, "something must have non-whitespace characters.", () ->
            ArgAssert.notEmpty("", "something")
        );

        expectExactMessage(IAE, "something must have non-whitespace characters.", () ->
            ArgAssert.notEmpty("   ", "something")
        );

        expectExactMessage(IAE, "something must have non-whitespace characters.", () ->
            ArgAssert.notEmpty("\n", "something")
        );

        expectExactMessage(IAE, "something must have non-whitespace characters.", () ->
            ArgAssert.notEmpty(System.lineSeparator(), "something")
        );

        assertEquals("not empty", ArgAssert.notEmpty("not empty", "Success"));
        assertEquals("not empty", ArgAssert.notEmpty("   not empty\n", "Success"));
    }

    @Test
    public void testIsFalse() throws Exception {
        expectExactMessage(IAE, "test message", () ->
            ArgAssert.isFalse(true, "test message")
        );

        ArgAssert.isFalse(false, "Success");
    }

    @Test
    public void testIsTrue() throws Exception {
        expectExactMessage(IAE, "test message", () ->
            ArgAssert.isTrue(false, "test message")
        );

        ArgAssert.isTrue(true, "Success");
    }

    @Test
    public void testPositive() {
        expectExactMessage(IAE, "testArg must be > 0.", () ->
            ArgAssert.positive(-1, "testArg")
        );

        expectExactMessage(IAE, "testArg must be > 0.", () ->
            ArgAssert.positive(0, "testArg")
        );

        ArgAssert.positive(10, "testArg");
    }

    @Test
    public void testPowerOfTwo() {
        expectExactMessage(IAE, "testArg must be a power of two.", () ->
            ArgAssert.powerOfTwo(13, "testArg")
        );

        expectExactMessage(IAE, "testArg must be a power of two.", () ->
            ArgAssert.powerOfTwo(0, "testArg")
        );

        expectExactMessage(IAE, "testArg must be a power of two.", () ->
            ArgAssert.powerOfTwo(-2, "testArg")
        );

        ArgAssert.powerOfTwo(8, "testArg");
    }
}
