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

package io.hekate.task.internal;

import io.hekate.core.Hekate;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.task.RemoteTaskException;
import io.hekate.task.TaskException;
import io.hekate.task.TaskFuture;
import io.hekate.task.TaskFutureException;
import io.hekate.task.TaskService;
import io.hekate.test.HekateTestError;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TaskRunTest extends TaskServiceTestBase {
    public TaskRunTest(MultiCodecTestContext params) {
        super(params);
    }

    @Test
    public void test() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<?> future = node.tasks().run(() -> {
                    NODES.add(node.localNode());

                    COUNTER.incrementAndGet();
                });

                get(future);
                future.get();
                future.getUninterruptedly();

                assertEquals(1, COUNTER.get());
                assertEquals(1, NODES.size());

                NODES.clear();
                COUNTER.set(0);
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testAffinity() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                get(node.tasks().withAffinity(100500).run(() -> {
                    NODES.add(node.localNode());

                    COUNTER.incrementAndGet();
                }));

                assertEquals(1, COUNTER.get());
                assertEquals(1, NODES.size());

                NODES.clear();
                COUNTER.set(0);
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testWithRuntimeError() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<?> future = node.tasks().run(() -> {
                    throw TEST_ERROR;
                });

                assertErrorCausedBy(future, RemoteTaskException.class, err -> {
                    assertEquals(TEST_ERROR.getClass(), err.getCause().getClass());
                    assertEquals(HekateTestError.MESSAGE, err.getCause().getMessage());
                });

            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testWithSerializationError() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 2);

            for (HekateTestNode node : nodes) {
                TaskFuture<?> future = node.tasks().forRemotes().run(() -> {
                    throw new NonSerializableTestException();
                });

                assertErrorCausedBy(future, TaskException.class, err -> {
                    MessagingRemoteException msgErr = (MessagingRemoteException)err.getCause();

                    assertTrue(msgErr.getMessage().contains(NonSerializableTestException.class.getName()));
                });
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testSourceLeave() throws Exception {
        List<HekateTestNode> nodes = createAndJoin(2);

        HekateTestNode source = nodes.get(0);
        HekateTestNode target = nodes.get(1);

        REF.set(source);

        TaskFuture<?> future = source.tasks().forRemotes().run(() -> {
            try {
                REF.get().leave();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });

        assertErrorCausedBy(future, ClosedChannelException.class);

        source.awaitForStatus(Hekate.State.DOWN);

        get(target.tasks().forNode(target.localNode()).run(() -> REF.set(target)));

        assertSame(target, REF.get());
    }

    @Test
    public void testFailover() throws Exception {
        List<HekateTestNode> nodes = createAndJoin(2);

        HekateTestNode source = nodes.get(0);

        AtomicInteger attempts = new AtomicInteger();

        TaskService tasks = source.tasks().forRemotes().withFailover(ctx -> {
            attempts.incrementAndGet();

            return ctx.attempt() < 5 ? ctx.retry() : ctx.fail();
        });

        // Successful retry.
        get(tasks.run(() -> {
            if (attempts.get() < 3) {
                throw TEST_ERROR;
            }
        }));

        assertEquals(3, attempts.get());

        attempts.set(0);

        // Failed retry.
        try {
            get(tasks.run(() -> {
                throw TEST_ERROR;
            }));

            fail("Error was expected.");
        } catch (TaskFutureException e) {
            assertSame(getStacktrace(e), RemoteTaskException.class, e.getCause().getClass());
            assertSame(getStacktrace(e), TEST_ERROR.getClass(), e.getCause().getCause().getClass());
        }

        assertEquals(6, attempts.get());
    }

    @Test
    public void testAffinityFailover() throws Exception {
        List<HekateTestNode> nodes = createAndJoin(2);

        HekateTestNode source = nodes.get(0);

        AtomicInteger attempts = new AtomicInteger();

        TaskService tasks = source.tasks().forRemotes().withFailover(ctx -> {
            attempts.incrementAndGet();

            return ctx.attempt() < 5 ? ctx.retry() : ctx.fail();
        });

        // Successful retry.
        get(tasks.withAffinity("some-affinity").run(() -> {
            if (attempts.get() < 3) {
                throw TEST_ERROR;
            }
        }));

        assertEquals(3, attempts.get());

        attempts.set(0);

        // Failed retry.
        try {
            get(tasks.withAffinity("some-affinity").run(() -> {
                throw TEST_ERROR;
            }));

            fail("Error was expected.");
        } catch (TaskFutureException e) {
            assertSame(getStacktrace(e), RemoteTaskException.class, e.getCause().getClass());
            assertSame(getStacktrace(e), TEST_ERROR.getClass(), e.getCause().getCause().getClass());
        }

        assertEquals(6, attempts.get());
    }

    @Test
    public void testFailoverReRoute() throws Exception {
        List<HekateTestNode> nodes = createAndJoin(3);

        HekateTestNode node = nodes.get(0);

        TaskService tasks = node.tasks().forRemotes().withFailover(ctx ->
            ctx.retry().withReRoute()
        );

        get(tasks.run(() -> {
            COUNTER.incrementAndGet();

            NODES.add(node.localNode());

            if (NODES.size() < 2) {
                throw TEST_ERROR;
            }
        }));

        assertTrue(NODES.contains(nodes.get(1).localNode()));
        assertTrue(NODES.contains(nodes.get(2).localNode()));

        assertEquals(2, COUNTER.get());
    }
}
