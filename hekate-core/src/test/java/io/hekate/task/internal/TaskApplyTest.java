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

import io.hekate.HekateTestContext;
import io.hekate.core.Hekate;
import io.hekate.core.HekateTestInstance;
import io.hekate.task.RemoteTaskException;
import io.hekate.task.TaskFuture;
import io.hekate.task.TaskFutureException;
import io.hekate.task.TaskService;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TaskApplyTest extends TaskServiceTestBase {
    public TaskApplyTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void test() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                int got = get(tasks.apply("test", arg -> {
                    NODES.add(node.getNode());

                    return COUNTER.incrementAndGet();
                }));

                assertEquals(1, got);
                assertEquals(1, NODES.size());

                COUNTER.set(0);
                NODES.clear();
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testAffinity() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                int got = get(tasks.withAffinity(100500).apply("test", arg -> {
                    NODES.add(node.getNode());

                    return COUNTER.incrementAndGet();
                }));

                assertEquals(1, got);
                assertEquals(1, NODES.size());

                COUNTER.set(0);
                NODES.clear();
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testNullResult() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                Object mustBeNull = get(tasks.apply("test", arg -> {
                    COUNTER.incrementAndGet();

                    return null;
                }));

                assertNull(mustBeNull);
                assertEquals(1, COUNTER.get());

                COUNTER.set(0);
                NODES.clear();
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testRuntimeException() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                TaskFuture<Object> future = tasks.apply("test", arg -> {
                    throw TEST_ERROR;
                });

                assertErrorCausedBy(future, RemoteTaskException.class, err -> {
                    assertTrue(err.getMessage().contains(TEST_ERROR.getClass().getName()));
                    assertTrue(err.getMessage().contains(TEST_ERROR.getMessage()));
                });
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testNonSerializable() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 2);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                TaskFuture<Object> future = tasks.forRemotes().apply("test", arg -> {
                    throw new NonSerializableTestException();
                });

                assertErrorCausedBy(future, RemoteTaskException.class, err ->
                    assertTrue(err.getMessage().contains(NonSerializableTestException.class.getName()))
                );
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testSourceLeave() throws Exception {
        List<HekateTestInstance> nodes = createAndJoin(2);

        HekateTestInstance source = nodes.get(0);
        HekateTestInstance target = nodes.get(1);

        REF.set(source);

        TaskFuture<?> future = source.get(TaskService.class).forRemotes().apply("test", arg -> {
            REF.get().leaveAsync().join();

            return null;
        });

        assertErrorCausedBy(future, ClosedChannelException.class);

        source.awaitForStatus(Hekate.State.DOWN);

        get(target.get(TaskService.class).forNode(target.getNode()).apply("test", arg -> {
            REF.set(target);

            return null;
        }));

        assertSame(target, REF.get());
    }

    @Test
    public void testFailover() throws Exception {
        List<HekateTestInstance> nodes = createAndJoin(2);

        HekateTestInstance source = nodes.get(0);

        AtomicInteger attempts = new AtomicInteger();

        TaskService tasks = source.get(TaskService.class).forRemotes().withFailover(ctx -> {
            attempts.incrementAndGet();

            return ctx.getAttempt() < 5 ? ctx.retry() : ctx.fail();
        });

        // Successful retry.
        assertEquals(
            "test-ok",
            get(tasks.apply("test", arg -> {
                if (attempts.get() < 3) {
                    throw TEST_ERROR;
                }

                return arg + "-ok";
            }))
        );

        assertEquals(3, attempts.get());

        attempts.set(0);

        // Failed retry.
        try {
            get(tasks.apply("test", arg -> {
                throw TEST_ERROR;
            }));

            fail("Error was expected.");
        } catch (TaskFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(RemoteTaskException.class));
            assertTrue(e.findCause(RemoteTaskException.class).getMessage().contains(TEST_ERROR_MESSAGE));
        }

        assertEquals(6, attempts.get());
    }

    @Test
    public void testAffinityFailover() throws Exception {
        List<HekateTestInstance> nodes = createAndJoin(2);

        HekateTestInstance source = nodes.get(0);

        AtomicInteger attempts = new AtomicInteger();

        TaskService tasks = source.get(TaskService.class).forRemotes().withFailover(ctx -> {
            attempts.incrementAndGet();

            return ctx.getAttempt() < 5 ? ctx.retry() : ctx.fail();
        });

        // Successful retry.
        assertEquals(
            "test-ok",
            get(tasks.withAffinity("some-affinity").apply("test", arg -> {
                if (attempts.get() < 3) {
                    throw TEST_ERROR;
                }

                return arg + "-ok";
            }))
        );

        assertEquals(3, attempts.get());

        attempts.set(0);

        // Failed retry.
        try {
            get(tasks.withAffinity("some-affinity").apply("test", arg -> {
                throw TEST_ERROR;
            }));

            fail("Error was expected.");
        } catch (TaskFutureException e) {
            assertTrue(getStacktrace(e), e.isCausedBy(RemoteTaskException.class));
            assertTrue(e.findCause(RemoteTaskException.class).getMessage().contains(TEST_ERROR_MESSAGE));
        }

        assertEquals(6, attempts.get());
    }

    @Test
    public void testFailoverReRoute() throws Exception {
        List<HekateTestInstance> nodes = createAndJoin(3);

        HekateTestInstance node = nodes.get(0);

        TaskService tasks = node.get(TaskService.class).forRemotes().withFailover(ctx ->
            ctx.retry().withReRoute()
        );

        assertEquals(
            "test-ok",
            get(tasks.apply("test", arg -> {
                COUNTER.incrementAndGet();

                NODES.add(node.getNode());

                if (NODES.size() < 2) {
                    throw TEST_ERROR;
                }

                return arg + "-ok";
            }))
        );

        assertTrue(NODES.contains(nodes.get(1).getNode()));
        assertTrue(NODES.contains(nodes.get(2).getNode()));

        assertEquals(2, COUNTER.get());
    }
}
