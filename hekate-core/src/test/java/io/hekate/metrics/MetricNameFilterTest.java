/*
 * Copyright 2018 The Hekate Project
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
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MetricNameFilterTest extends HekateTestBase {
    @Test
    public void test() {
        MetricNameFilter filter = new MetricNameFilter("test");

        assertEquals("test", filter.metricName());

        assertTrue(filter.accept(new MetricValue("test", 10)));

        assertFalse(filter.accept(new MetricValue("test.1", 10)));
        assertFalse(filter.accept(new MetricValue("1.test.1", 10)));
        assertFalse(filter.accept(new MetricValue("1.test", 10)));
    }

    @Test
    public void testToString() {
        MetricNameFilter filter = new MetricNameFilter("test");

        assertEquals(ToString.format(filter), filter.toString());
    }
}
