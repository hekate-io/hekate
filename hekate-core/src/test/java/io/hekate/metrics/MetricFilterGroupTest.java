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
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MetricFilterGroupTest extends HekateTestBase {
    @Test
    public void testFilters() {
        MetricFilter f1 = metric -> true;
        MetricFilter f2 = metric -> true;
        MetricFilter f3 = metric -> true;

        MetricFilterGroup group = new MetricFilterGroup();

        assertNull(group.getFilters());

        group.setFilters(new ArrayList<>(Arrays.asList(f1, f2)));

        assertEquals(2, group.getFilters().size());
        assertTrue(group.getFilters().contains(f1));
        assertTrue(group.getFilters().contains(f2));

        assertSame(group, group.withFilter(f3));

        assertEquals(3, group.getFilters().size());
        assertTrue(group.getFilters().contains(f1));
        assertTrue(group.getFilters().contains(f2));
        assertTrue(group.getFilters().contains(f3));

        group.setFilters(null);

        assertNull(group.getFilters());

        assertSame(group, group.withFilter(f3));

        assertEquals(1, group.getFilters().size());
        assertTrue(group.getFilters().contains(f3));
    }

    @Test
    public void testAccept() {
        MetricFilter f1 = metric -> false;
        MetricFilter f2 = metric -> true;

        MetricFilterGroup group = new MetricFilterGroup().withFilter(f1);

        Metric m = mock(Metric.class);

        assertFalse(group.accept(m));

        group.withFilter(f2);

        assertTrue(group.accept(m));
    }
}
