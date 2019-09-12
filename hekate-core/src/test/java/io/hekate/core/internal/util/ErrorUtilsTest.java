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
