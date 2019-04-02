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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class JvmTest extends HekateTestBase {
    @Test
    public void testPid() {
        String pid1 = Jvm.pid();
        String pid2 = Jvm.pid();

        assertNotNull(pid1);
        assertEquals(pid1, pid2);
    }

    @Test
    public void testExitHandler() {
        assertFalse(Jvm.exitHandler().isPresent());

        try {
            Jvm.ExitHandler handler = mock(Jvm.ExitHandler.class);

            Jvm.setExitHandler(handler);

            assertTrue(Jvm.exitHandler().isPresent());

            Jvm.exit(100500);

            verify(handler).exit(100500);
            verifyNoMoreInteractions(handler);
        } finally {
            Jvm.setExitHandler(null);

            assertNotNull(Jvm.exitHandler());
            assertFalse(Jvm.exitHandler().isPresent());
        }
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(Jvm.class);
    }
}
