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
import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.UnknownRouteException;
import io.hekate.task.RemoteTaskException;
import io.hekate.task.TaskException;
import io.hekate.task.TaskFuture;
import io.hekate.task.TaskService;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TaskApplyToAllTest extends TaskServiceTestBase {
    public TaskApplyToAllTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void test() throws Exception {
        repeat(5, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                sayHeader("Applying on " + node.localNode().name() + " [topology=" + node.cluster().topology());

                repeat(10, j -> {
                    // Prepare arguments.
                    List<Integer> args = new ArrayList<>();

                    for (int k = 0; k < j + nodes.size(); k++) {
                        args.add(k);
                    }

                    // Apply.
                    Collection<String> results = get(node.tasks().applyToAll(args, arg ->
                        node.localNode().name() + '=' + arg
                    ));

                    say("Results: " + results);

                    // Map to results to arguments.
                    Map<String, Integer> map = results.stream().collect(toMap(identity(), v ->
                        Integer.parseInt(v.split("=")[1]))
                    );

                    List<String> uniqueNodes = map.keySet().stream()
                        .map(s -> s.split("=")[0])
                        .distinct()
                        .collect(toList());

                    say("Unique nodes: " + uniqueNodes);

                    assertEquals(i + 1, uniqueNodes.size());

                    for (int k = 0; k < args.size(); k++) {
                        assertEquals(args.get(k), map.values().stream()
                            .distinct()
                            .sorted()
                            .collect(toList())
                            .get(k));
                    }
                });
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testNullResult() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                Collection<Object> allNulls = get(node.tasks().applyToAll(asList(1, 2, 3), arg -> null));

                assertNotNull(allNulls);
                assertEquals(3, allNulls.size());
                assertTrue(allNulls.stream().allMatch(Objects::isNull));
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testCheckedException() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<Collection<Object>> future = node.tasks().applyToAll(Arrays.asList(1, 2, 3), arg -> {
                    throw new TaskException(TEST_ERROR_MESSAGE);
                });

                assertErrorCausedBy(future, RemoteTaskException.class, err -> {
                    assertTrue(err.getMessage().contains(TaskException.class.getName()));
                    assertTrue(err.getMessage().contains(TEST_ERROR_MESSAGE));
                });
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testPartialCheckedException() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<Collection<Object>> future = node.tasks().applyToAll(Arrays.asList(1, 2, 3), arg -> {
                    if (arg == 2) {
                        throw new TaskException(TEST_ERROR_MESSAGE);
                    } else {
                        return arg;
                    }
                });

                assertErrorCausedBy(future, RemoteTaskException.class, err -> {
                    assertTrue(err.getMessage().contains(TaskException.class.getName()));
                    assertTrue(err.getMessage().contains(TEST_ERROR_MESSAGE));
                });
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testRuntimeException() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<Collection<Object>> future = node.tasks().applyToAll(Arrays.asList(1, 2, 3), arg -> {
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
    public void testPartialRuntimeException() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<Collection<Object>> future = node.tasks().applyToAll(Arrays.asList(1, 2, 3), arg -> {
                    if (arg == 2) {
                        throw TEST_ERROR;
                    } else {
                        return arg;
                    }
                });

                assertErrorCausedBy(future, RemoteTaskException.class, err -> {
                    assertTrue(err.getMessage().contains(TEST_ERROR.getClass().getName()));
                    assertTrue(err.getMessage().contains(TEST_ERROR.getMessage()));

                    RemoteTaskException remote = (RemoteTaskException)err;

                    assertTrue(remote.getRemoteStackTrace(), remote.getRemoteStackTrace().contains(TEST_ERROR.getClass().getName()));
                });
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testFailOnEmptyTopology() throws Exception {
        HekateTestNode node = createAndJoin(1).get(0);

        TaskFuture<Collection<Integer>> future = node.tasks().forRemotes().applyToAll(Arrays.asList(1, 2, 3), arg -> arg);

        assertErrorCausedBy(future, UnknownRouteException.class, err ->
            assertEquals("No suitable task executors in the cluster topology.", err.getMessage())
        );
    }

    @Test
    public void testNonSerializableException() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                TaskFuture<Collection<Object>> future = node.tasks().applyToAll(Arrays.asList(1, 2, 3), arg -> {
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
        List<HekateTestNode> nodes = createAndJoin(2);

        HekateTestNode source = nodes.get(0);
        HekateTestNode target = nodes.get(1);

        REF.set(source);

        TaskFuture<?> future = source.tasks().forRemotes().applyToAll(Arrays.asList(1, 2, 3), arg -> {
            REF.get().leaveAsync().join();

            return null;
        });

        assertErrorCausedBy(future, ClosedChannelException.class);

        source.awaitForStatus(Hekate.State.DOWN);

        get(target.tasks().forNode(target.localNode()).applyToAll(asList(1, 2, 3), arg -> {
            REF.set(target);

            return null;
        }));

        assertSame(target, REF.get());
    }

    @Test
    public void testFailover() throws Exception {
        repeat(3, i -> {
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                AtomicInteger attempts = new AtomicInteger();

                TaskService tasks = node.tasks().withFailover(ctx -> {
                    attempts.incrementAndGet();

                    return ctx.retry();
                });

                TaskFuture<Collection<Object>> future = tasks.applyToAll(Arrays.asList(1, 2, 3), arg -> {
                    if (arg == 2 && (attempts.get() == 0 || ThreadLocalRandom.current().nextBoolean())) {
                        throw new TaskException(TEST_ERROR_MESSAGE);
                    } else {
                        return arg;
                    }
                });

                get(future).containsAll(Arrays.asList(1, 2, 3));

                assertTrue(attempts.get() > 0);
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }
}
