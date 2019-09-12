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

package io.hekate.core.internal;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.cluster.event.ClusterLeaveReason;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.core.Hekate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ClusterEventManagerTest extends HekateTestBase {
    private static class TestListener implements ClusterEventListener {
        private final List<ClusterEvent> events = new CopyOnWriteArrayList<>();

        @Override
        public void onEvent(ClusterEvent event) {
            events.add(event);
        }

        public List<ClusterEvent> getEvents() {
            return events;
        }
    }

    private static class TestThreadFactory implements ThreadFactory {
        private final AtomicBoolean singleThread = new AtomicBoolean();

        @Override
        public Thread newThread(Runnable r) {
            if (singleThread.compareAndSet(false, true)) {
                return new Thread(r);
            }

            throw new AssertionError("Test thread factory can create only single thread.");
        }

        public void reset() {
            singleThread.set(false);
        }
    }

    private static final ClusterEventType[] EMPTY_EVENT_TYPES = new ClusterEventType[0];

    private final ClusterEventManager mgr = new ClusterEventManager(mock(Hekate.class));

    private final TestThreadFactory threads = new TestThreadFactory();

    @After
    public void tearDown() throws InterruptedException {
        mgr.stop();
    }

    @Test
    public void testAddRemoveListenersBeforeStart() throws Exception {
        TestListener listener = new TestListener();

        mgr.addListener(listener);
        mgr.removeListener(listener);

        mgr.start(threads);

        mgr.fireAsync(newJoinEvent());

        mgr.stop();

        assertEquals(0, listener.getEvents().size());
    }

    @Test
    public void testAddListenersBeforeStart() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(3, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testBeforeStartListenersRemoveOnStop() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            threads.reset();

            mgr.start(threads);

            mgr.stop();

            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(0, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testAfterStartListenersRemoveOnStop() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(threads);

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            mgr.stop();

            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(0, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testAddListenersAfterStart() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(threads);

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(3, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testAddListenersAfterInitialEvent() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(1, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testWorkAfterError() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(new TestThreadFactory());

            mgr.addListener(e -> {
                throw TEST_ERROR;
            });

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(3, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testWorkAfterErrorWithInitEvent() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newJoinEvent());

            mgr.addListener(e -> {
                throw TEST_ERROR;
            });

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener);
            }

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(1, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testAddListenerWithFilterBeforeStart() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener, ClusterEventType.CHANGE);
            }

            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());
            mgr.fireAsync(newChangeEvent());
            mgr.fireAsync(newChangeEvent());
            mgr.fireAsync(newLeaveEvent());

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(2, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testAddListenerWithFilterAfterStart() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener, ClusterEventType.CHANGE);
            }

            mgr.fireAsync(newChangeEvent());
            mgr.fireAsync(newChangeEvent());
            mgr.fireAsync(newLeaveEvent());

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(2, listener.getEvents().size());
            }
        }
    }

    @Test
    public void testAddListenerWithEmptyFilterAfterStart() throws Exception {
        List<TestListener> listeners = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            threads.reset();

            mgr.start(threads);

            mgr.fireAsync(newJoinEvent());

            for (int j = 0; j < i; j++) {
                TestListener listener = new TestListener();

                listeners.add(listener);

                mgr.addListener(listener, EMPTY_EVENT_TYPES);
            }

            mgr.fireAsync(newChangeEvent());
            mgr.fireAsync(newChangeEvent());
            mgr.fireAsync(newLeaveEvent());

            listeners.forEach(mgr::removeListener);

            mgr.stop();

            for (TestListener listener : listeners) {
                assertEquals(4, listener.getEvents().size());
            }
        }
    }

    private ClusterJoinEvent newJoinEvent() throws Exception {
        return new ClusterJoinEvent(newTopology(), mock(Hekate.class));
    }

    private ClusterChangeEvent newChangeEvent() throws Exception {
        ClusterTopology topology = newTopology();

        return new ClusterChangeEvent(topology, topology.nodes(), emptyList(), emptyList(), mock(Hekate.class));
    }

    private ClusterLeaveEvent newLeaveEvent() throws Exception {
        ClusterTopology topology = newTopology();

        Hekate hekate = mock(Hekate.class);

        return new ClusterLeaveEvent(ClusterLeaveReason.LEAVE, topology, emptyList(), singletonList(topology.localNode()), hekate);
    }

    private ClusterTopology newTopology() throws Exception {
        return DefaultClusterTopology.of(1, toSet(newNode(), newNode(), newNode()));
    }
}
