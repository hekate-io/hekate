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

package io.hekate.core.internal;

import io.hekate.HekateInstanceTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateTestInstance;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HekateLifecycleListenerTest extends HekateInstanceTestBase {
    private HekateTestInstance node;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        node = createInstance();
    }

    @Test
    public void testRegisterUnregister() throws Exception {
        repeat(3, i -> {
            List<Hekate.State> state = Collections.synchronizedList(new ArrayList<>());

            Hekate.LifecycleListener listener = hekate -> state.add(hekate.getState());

            node.addListener(listener);

            node.join();

            assertSame(Hekate.State.INITIALIZING, state.get(0));
            assertSame(Hekate.State.JOINING, state.get(1));
            assertSame(Hekate.State.UP, state.get(2));

            state.clear();

            node.leave();

            assertSame(Hekate.State.LEAVING, state.get(0));
            assertSame(Hekate.State.TERMINATING, state.get(1));
            assertSame(Hekate.State.DOWN, state.get(2));

            assertTrue(node.removeListener(listener));
        });
    }

    @Test
    public void testRemoveUnknownListener() throws Exception {
        assertFalse(node.removeListener(changed -> {
            // No-op.
        }));

        node.addListener(changed -> {
            // No-op.
        });

        assertFalse(node.removeListener(changed -> {
            // No-op.
        }));
    }

    @Test
    public void testErrorInListener() throws Exception {
        repeat(3, i -> {
            List<Hekate.State> state = Collections.synchronizedList(new ArrayList<>());

            Hekate.LifecycleListener listener = hekate -> {
                state.add(hekate.getState());

                throw TEST_ERROR;
            };

            node.addListener(listener);

            node.join();

            assertSame(Hekate.State.INITIALIZING, state.get(0));
            assertSame(Hekate.State.JOINING, state.get(1));
            assertSame(Hekate.State.UP, state.get(2));

            state.clear();

            node.leave();

            assertSame(Hekate.State.LEAVING, state.get(0));
            assertSame(Hekate.State.TERMINATING, state.get(1));
            assertSame(Hekate.State.DOWN, state.get(2));

            assertTrue(node.removeListener(listener));
        });
    }

    @Test
    public void testLeaveFromListener() throws Exception {
        List<Hekate.State> states = Arrays.asList(
            Hekate.State.INITIALIZING,
            Hekate.State.JOINING,
            Hekate.State.UP,
            Hekate.State.LEAVING,
            Hekate.State.TERMINATING,
            Hekate.State.DOWN
        );

        for (Hekate.State state : states) {
            say("Testing state: " + state);

            CountDownLatch notified = new CountDownLatch(1);

            Hekate.LifecycleListener listener = changed -> {
                if (changed.getState() == state) {
                    notified.countDown();

                    changed.leaveAsync();
                }
            };

            node.addListener(listener);

            node.join();

            if (state == Hekate.State.LEAVING || state == Hekate.State.TERMINATING || state == Hekate.State.DOWN) {
                node.leaveAsync();
            }

            node.awaitForStatus(Hekate.State.DOWN);

            await(notified);

            assertTrue(node.removeListener(listener));

            node.leave();
        }
    }

    @Test
    public void testTerminateFromListener() throws Throwable {
        List<Hekate.State> states = Arrays.asList(
            Hekate.State.INITIALIZING,
            Hekate.State.JOINING,
            Hekate.State.UP,
            Hekate.State.LEAVING,
            Hekate.State.TERMINATING,
            Hekate.State.DOWN
        );

        for (Hekate.State state : states) {
            say("Testing state: " + state);

            CountDownLatch notified = new CountDownLatch(1);

            AtomicReference<Throwable> errorRef = new AtomicReference<>();

            Hekate.LifecycleListener listener = changed -> {
                try {
                    if (changed.getState() == state) {
                        notified.countDown();

                        changed.terminateAsync();
                    }
                } catch (Throwable t) {
                    errorRef.compareAndSet(null, t);
                }
            };

            node.addListener(listener);

            node.join();

            if (state == Hekate.State.LEAVING || state == Hekate.State.TERMINATING || state == Hekate.State.DOWN) {
                node.leaveAsync();
            }

            node.awaitForStatus(Hekate.State.DOWN);

            await(notified);

            assertTrue(node.removeListener(listener));

            node.leave();

            if (errorRef.get() != null) {
                throw errorRef.get();
            }
        }
    }
}
