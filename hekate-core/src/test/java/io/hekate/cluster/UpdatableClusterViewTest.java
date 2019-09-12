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

package io.hekate.cluster;

import io.hekate.HekateTestBase;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.util.format.ToString;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class UpdatableClusterViewTest extends HekateTestBase {
    @Test
    public void testEmpty() {
        UpdatableClusterView view = UpdatableClusterView.empty();

        assertTrue(view.topology().isEmpty());
        assertEquals(0, view.topology().version());
    }

    @Test
    public void testOfTopology() throws Exception {
        ClusterTopology topology = ClusterTopology.of(1, toSet(newNode(), newNode()));

        UpdatableClusterView view = UpdatableClusterView.of(topology);

        assertSame(topology, view.topology());
    }

    @Test
    public void testOfOneNode() throws Exception {
        ClusterNode node = newNode();

        UpdatableClusterView view = UpdatableClusterView.of(100500, node);

        assertEquals(100500, view.topology().version());
        assertEquals(1, view.topology().size());
        assertTrue(view.topology().contains(node));
    }

    @Test
    public void testOfNodeSet() throws Exception {
        ClusterNode node1 = newNode();
        ClusterNode node2 = newNode();

        UpdatableClusterView view = UpdatableClusterView.of(100500, toSet(node1, node2));

        assertEquals(100500, view.topology().version());
        assertEquals(2, view.topology().size());
        assertTrue(view.topology().contains(node1));
        assertTrue(view.topology().contains(node2));
    }

    @Test
    public void testUpdate() throws Exception {
        UpdatableClusterView view = UpdatableClusterView.empty();

        ClusterTopology t1 = ClusterTopology.of(1, toSet(newNode(), newNode()));
        ClusterTopology t1SameVer = ClusterTopology.of(1, toSet(newNode(), newNode()));
        ClusterTopology t2 = ClusterTopology.of(2, toSet(newNode(), newNode()));

        view.update(t1);

        assertSame(t1, view.topology());

        // Should not update since new topology has the same version with the old one.
        view.update(t1SameVer);

        assertSame(t1, view.topology());

        // Should update.
        view.update(t2);

        assertSame(t2, view.topology());

        // Should not update since new topology has older version than the old one.
        view.update(t1);

        assertSame(t2, view.topology());
    }

    @Test
    public void testUnsupportedMethods() {
        UpdatableClusterView view = UpdatableClusterView.empty();

        expect(UnsupportedOperationException.class, () -> view.addListener(mock(ClusterEventListener.class)));
        expect(UnsupportedOperationException.class, () -> view.addListener(mock(ClusterEventListener.class), ClusterEventType.CHANGE));
        expect(UnsupportedOperationException.class, () -> view.removeListener(mock(ClusterEventListener.class)));
        expect(UnsupportedOperationException.class, () -> view.futureOf(topology -> true));
        expect(UnsupportedOperationException.class, () -> view.awaitFor(topology -> true));
        expect(UnsupportedOperationException.class, () -> view.awaitFor(topology -> true, 1, TimeUnit.SECONDS));
    }

    @Test
    public void testFilter() throws Exception {
        ClusterNode n1 = newNode();
        ClusterNode n2 = newNode();

        ClusterTopology topology = ClusterTopology.of(1, toSet(n1, n2));

        UpdatableClusterView view = UpdatableClusterView.of(topology);

        ClusterView filter = view.filter(n -> n != n1);

        assertEquals(topology.version(), filter.topology().version());
        assertEquals(Collections.singletonList(n2), filter.topology().nodes());
    }

    @Test
    public void testToString() {
        UpdatableClusterView view = UpdatableClusterView.empty();

        assertEquals(ToString.format(view), view.toString());
    }
}
