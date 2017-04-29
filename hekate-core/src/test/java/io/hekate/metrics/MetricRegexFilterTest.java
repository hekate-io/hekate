/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.metrics;

import io.hekate.HekateTestBase;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetricRegexFilterTest extends HekateTestBase {
    @Test
    public void test() {
        String pattern = "test\\.test\\..*";

        MetricRegexFilter filter = new MetricRegexFilter(pattern);

        assertEquals(pattern, filter.getPattern());

        OngoingStubbing<String> stub = when(mock(Metric.class).name());

        assertTrue(filter.accept(stub.thenReturn("test.test.1").getMock()));
        assertTrue(filter.accept(stub.thenReturn("test.test.2").getMock()));

        assertFalse(filter.accept(stub.thenReturn("test1").getMock()));
        assertFalse(filter.accept(stub.thenReturn("test1test.2").getMock()));
        assertFalse(filter.accept(stub.thenReturn("invalid").getMock()));
        assertFalse(filter.accept(stub.thenReturn("invalid.test").getMock()));
    }
}
