/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.util.trace;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class TraceInfoTest extends HekateTestBase {
    @Test
    public void test() {
        TraceInfo trace = TraceInfo.of("test");

        assertEquals("test", trace.name());
        assertNull(trace.tags());

        assertSame(trace, trace.withTag("t", "v"));

        assertNotNull(trace.tags());
        assertEquals(Collections.singletonMap("t", "v"), trace.tags());

        assertEquals(ToString.format(trace), trace.toString());
    }
}
