/*
 * Copyright 2017 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http:www.apache.orglicensesLICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.cluster.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.core.internal.HekateTestNode;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static java.util.Collections.singleton;
import static org.hamcrest.CoreMatchers.both;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class FilteredClusterViewTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        HekateTestNode node1 = createNode(c -> c.withNodeRole("role1").withNodeRole("all")).join();
        HekateTestNode node2 = createNode(c -> c.withNodeRole("role2").withNodeRole("all")).join();
        HekateTestNode node3 = createNode(c -> c.withNodeRole("role3").withNodeRole("all")).join();

        ClusterView allRemote = node1.cluster().forRemotes().forRole("all");

        get(allRemote.futureOf(topology -> topology.size() == 2));

        assertTrue(allRemote.toString().startsWith(FilteredClusterView.class.getSimpleName()));
        assertTrue(allRemote.toString().contains(node2.getLocalNode().toString()));
        assertTrue(allRemote.toString().contains(node3.getLocalNode().toString()));

        assertThat(allRemote.getTopology().getNodes(),
            both(hasItems(node2.getLocalNode(), node3.getLocalNode()))
                .and(not(hasItem(node1.getLocalNode())))
        );
        assertThat(allRemote.forRole("role2").getTopology().getNodes(),
            both(hasItem(node2.getLocalNode()))
                .and(not(hasItem(node3.getLocalNode())))
        );
        
        assertTrue(node1.cluster().forRemotes().forNode(node1.getLocalNode()).getTopology().isEmpty());

        ClusterEventListener l1 = mock(ClusterEventListener.class);
        ClusterEventListener l2 = mock(ClusterEventListener.class);
        ClusterEventListener l3 = mock(ClusterEventListener.class);
        ClusterEventListener l4 = mock(ClusterEventListener.class);

        allRemote.addListener(l1);
        allRemote.forRole("role4").addListener(l2);
        allRemote.forRole("none").addListener(l3);
        allRemote.addListener(l4, ClusterEventType.CHANGE);

        verify(l1).onEvent(Mockito.any(ClusterJoinEvent.class));
        verify(l2).onEvent(Mockito.any(ClusterJoinEvent.class));
        verify(l3).onEvent(Mockito.any(ClusterJoinEvent.class));
        verifyNoMoreInteractions(l4);

        reset(l1);
        reset(l2);
        reset(l3);
        reset(l4);

        HekateTestNode node4 = createNode(c -> c.withNodeRole("role4").withNodeRole("all")).join();

        get(node1.cluster().forRemotes().forRole("role4").futureOf(topology -> !topology.isEmpty()));

        ArgumentCaptor<ClusterChangeEvent> evt1 = ArgumentCaptor.forClass(ClusterChangeEvent.class);
        ArgumentCaptor<ClusterChangeEvent> evt2 = ArgumentCaptor.forClass(ClusterChangeEvent.class);
        ArgumentCaptor<ClusterChangeEvent> evt3 = ArgumentCaptor.forClass(ClusterChangeEvent.class);
        ArgumentCaptor<ClusterChangeEvent> evt4 = ArgumentCaptor.forClass(ClusterChangeEvent.class);

        verify(l1).onEvent(evt1.capture());
        verify(l2).onEvent(evt2.capture());
        verify(l3).onEvent(evt3.capture());
        verify(l4).onEvent(evt4.capture());

        assertThat(evt1.getValue().getTopology().getNodes(),
            both(hasItems(node2.getLocalNode(), node3.getLocalNode(), node4.getLocalNode()))
                .and(not(hasItem(node1.getLocalNode())))
        );
        assertEquals(evt1.getValue().getAdded(), singleton(node4.getLocalNode()));
        assertTrue(evt1.getValue().getRemoved().isEmpty());

        assertEquals(evt2.getValue().getTopology().getNodes(), singleton(node4.getLocalNode()));
        assertEquals(evt2.getValue().getAdded(), singleton(node4.getLocalNode()));
        assertTrue(evt2.getValue().getRemoved().isEmpty());

        assertTrue(evt3.getValue().getTopology().isEmpty());
        assertTrue(evt3.getValue().getRemoved().isEmpty());
        assertTrue(evt3.getValue().getRemoved().isEmpty());

        assertEquals(evt1.getValue().getTopology(), evt4.getValue().getTopology());

        reset(l1);
        reset(l2);
        reset(l3);

        node4.leave();

        get(node1.cluster().forRemotes().forRole("role4").futureOf(ClusterTopology::isEmpty));

        verify(l1).onEvent(evt1.capture());
        verify(l2).onEvent(evt2.capture());
        verify(l3).onEvent(evt3.capture());

        assertThat(evt1.getValue().getTopology().getNodes(),
            both(hasItems(node2.getLocalNode(), node3.getLocalNode()))
                .and(not(hasItem(node1.getLocalNode())))
                .and(not(hasItem(node4.getLocalNode())))
        );
        assertTrue(evt1.getValue().getAdded().isEmpty());
        assertEquals(evt1.getValue().getRemoved(), singleton(node4.getLocalNode()));

        assertTrue(evt2.getValue().getTopology().isEmpty());
        assertEquals(evt2.getValue().getRemoved(), singleton(node4.getLocalNode()));
        assertTrue(evt2.getValue().getAdded().isEmpty());

        assertTrue(evt3.getValue().getTopology().isEmpty());
        assertTrue(evt3.getValue().getRemoved().isEmpty());
        assertTrue(evt3.getValue().getRemoved().isEmpty());

        allRemote.removeListener(l1);
        allRemote.removeListener(l2);
        allRemote.removeListener(l3);
        allRemote.removeListener(l4);

        reset(l1);
        reset(l2);
        reset(l3);
        reset(l4);

        node2.leave();

        awaitForTopology(node1, node3);

        verifyNoMoreInteractions(l1);
        verifyNoMoreInteractions(l2);
        verifyNoMoreInteractions(l3);
        verifyNoMoreInteractions(l4);
    }
}
