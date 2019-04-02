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

package io.hekate.cluster.split;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.HekateConfigurationException;
import io.hekate.test.SplitBrainDetectorMock;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SplitBrainDetectorGroupTest extends HekateTestBase {
    private final SplitBrainDetectorGroup group = new SplitBrainDetectorGroup();

    @Test
    public void testEmptyAnyPolicy() throws Exception {
        assertTrue(group.withGroupPolicy(SplitBrainDetectorGroup.GroupPolicy.ANY_VALID).isValid(newNode()));
    }

    @Test
    public void testEmptyAllPolicy() throws Exception {
        assertTrue(group.withGroupPolicy(SplitBrainDetectorGroup.GroupPolicy.ALL_VALID).isValid(newNode()));
    }

    @Test
    public void testAnyValidPolicy() throws Exception {
        group.withGroupPolicy(SplitBrainDetectorGroup.GroupPolicy.ANY_VALID);

        SplitBrainDetectorMock d1 = new SplitBrainDetectorMock(true);
        SplitBrainDetectorMock d2 = new SplitBrainDetectorMock(true);
        SplitBrainDetectorMock d3 = new SplitBrainDetectorMock(true);

        group.setDetectors(Arrays.asList(d1, d2, d3));

        ClusterNode node = newNode();

        assertTrue(group.isValid(node));

        d1.setValid(false);

        assertTrue(group.isValid(node));

        d2.setValid(false);

        assertTrue(group.isValid(node));

        d3.setValid(false);

        assertFalse(group.isValid(node));
    }

    @Test
    public void testAllValidPolicy() throws Exception {
        group.withGroupPolicy(SplitBrainDetectorGroup.GroupPolicy.ALL_VALID);

        SplitBrainDetectorMock d1 = new SplitBrainDetectorMock(true);
        SplitBrainDetectorMock d2 = new SplitBrainDetectorMock(true);
        SplitBrainDetectorMock d3 = new SplitBrainDetectorMock(true);

        group.setDetectors(Arrays.asList(d1, d2, d3));

        ClusterNode node = newNode();

        assertTrue(group.isValid(node));

        d1.setValid(false);

        assertFalse(group.isValid(node));

        d1.setValid(true);
        d2.setValid(false);

        assertFalse(group.isValid(node));

        d2.setValid(true);
        d3.setValid(false);

        assertFalse(group.isValid(node));
    }

    @Test
    public void testSetDetectors() {
        assertNull(group.getDetectors());

        SplitBrainDetectorMock d1 = new SplitBrainDetectorMock(true);
        SplitBrainDetectorMock d2 = new SplitBrainDetectorMock(true);

        group.setDetectors(Arrays.asList(d1, d2));

        assertEquals(2, group.getDetectors().size());
        assertTrue(group.getDetectors().contains(d1));
        assertTrue(group.getDetectors().contains(d2));

        group.setDetectors(null);

        assertNull(group.getDetectors());

        assertSame(group, group.withDetector(d1));

        assertEquals(1, group.getDetectors().size());
        assertTrue(group.getDetectors().contains(d1));
    }

    @Test
    public void testSetPolicy() {
        assertSame(SplitBrainDetectorGroup.GroupPolicy.ANY_VALID, group.getGroupPolicy());

        group.setGroupPolicy(SplitBrainDetectorGroup.GroupPolicy.ALL_VALID);

        assertSame(SplitBrainDetectorGroup.GroupPolicy.ALL_VALID, group.getGroupPolicy());

        assertSame(group, group.withGroupPolicy(SplitBrainDetectorGroup.GroupPolicy.ANY_VALID));

        assertSame(SplitBrainDetectorGroup.GroupPolicy.ANY_VALID, group.getGroupPolicy());

        try {
            group.setGroupPolicy(null);

            fail("Failure was expected.");
        } catch (HekateConfigurationException e) {
            assertTrue(e.getMessage().contains("group policy must be not null"));
        }
    }
}
