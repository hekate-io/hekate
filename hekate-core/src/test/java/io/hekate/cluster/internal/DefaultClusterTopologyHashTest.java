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

package io.hekate.cluster.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopologyHash;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class DefaultClusterTopologyHashTest extends HekateTestBase {
    @Test
    public void test() throws Exception {
        ClusterNode n1 = newNode();
        ClusterNode n2 = newNode();
        ClusterNode n3 = newNode();

        ClusterTopologyHash h1 = new DefaultClusterTopologyHash(Collections.emptyList());

        ClusterTopologyHash h2 = new DefaultClusterTopologyHash(Collections.singleton(n1));
        ClusterTopologyHash h3 = new DefaultClusterTopologyHash(Collections.singleton(n1));

        ClusterTopologyHash h4 = new DefaultClusterTopologyHash(Arrays.asList(n1, n2, n3));
        ClusterTopologyHash h5 = new DefaultClusterTopologyHash(Arrays.asList(n3, n2, n1));

        assertNotEquals(h1, h2);
        assertNotEquals(h2, h4);

        assertEquals(h2, h3);
        assertEquals(h4, h5);
    }
}
