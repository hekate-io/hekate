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
import io.hekate.cluster.ClusterService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateTestInstance;
import io.hekate.core.internal.HekateInstance;
import io.hekate.task.MultiNodeResult;
import io.hekate.task.TaskService;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TaskAggregateTest extends TaskServiceTestBase {
    public TaskAggregateTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void test() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                MultiNodeResult<Integer> result = tasks.aggregate(() -> {
                    int intResult = COUNTER.incrementAndGet();

                    NODES.add(node.getNode());

                    return intResult;
                }).get(3, TimeUnit.SECONDS);

                List<Integer> sorted = result.getResults().values().stream().sorted().collect(toList());

                assertEquals(nodes.size(), COUNTER.get());
                assertTrue(NODES.toString(), NODES.containsAll(nodes.stream().map(HekateInstance::getNode).collect(toList())));
                assertTrue(result.isSuccess());
                nodes.forEach(n -> assertTrue(result.isSuccess(n.getNode())));
                assertTrue(result.getNodes().containsAll(nodes.stream().map(HekateInstance::getNode).collect(toList())));

                assertEquals(sorted.size(), nodes.size());

                for (int j = 0; j < sorted.size(); j++) {
                    assertEquals(j + 1, sorted.get(j).intValue());
                }

                NODES.clear();
                COUNTER.set(0);
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

                MultiNodeResult<Integer> affResult = tasks.aggregate(100500, () -> {
                    int intResult = COUNTER.incrementAndGet();

                    NODES.add(node.getNode());

                    return intResult;
                }).get(3, TimeUnit.SECONDS);

                List<Integer> affSorted = affResult.getResults().values().stream().sorted().collect(toList());

                assertEquals(nodes.size(), COUNTER.get());
                assertTrue(NODES.toString(), NODES.containsAll(nodes.stream().map(HekateInstance::getNode).collect(toList())));
                assertTrue(affResult.isSuccess());
                nodes.forEach(n -> assertTrue(affResult.isSuccess(n.getNode())));
                assertTrue(affResult.getNodes().containsAll(nodes.stream().map(HekateInstance::getNode).collect(toList())));

                assertEquals(affSorted.size(), nodes.size());

                for (int j = 0; j < affSorted.size(); j++) {
                    assertEquals(j + 1, affSorted.get(j).intValue());
                }

                NODES.clear();
                COUNTER.set(0);
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testError() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                MultiNodeResult<?> errResult = tasks.aggregate(() -> {
                    throw TEST_ERROR;
                }).get(3, TimeUnit.SECONDS);

                assertFalse(errResult.isSuccess());
                assertTrue(errResult.getResults().isEmpty());
                nodes.forEach(n -> {
                    assertFalse(errResult.isSuccess(n.getNode()));
                    assertNotNull(errResult.getError(n.getNode()));

                    assertTrue(errResult.getError(n.getNode()).getMessage().contains(TestAssertionError.class.getName()));
                    assertTrue(errResult.getError(n.getNode()).getMessage().contains(TEST_ERROR_MESSAGE));
                });

                NODES.clear();
                COUNTER.set(0);
            }

            nodes.forEach(n -> n.leaveAsync().join());
        });
    }

    @Test
    public void testPartialError() throws Exception {
        repeat(3, i -> {
            List<HekateTestInstance> nodes = createAndJoin(i + 1);

            for (HekateTestInstance node : nodes) {
                TaskService tasks = node.get(TaskService.class);

                MultiNodeResult<Integer> partErrResult = tasks.aggregate(() -> {
                    if (node.get(ClusterService.class).getTopology().getYoungest().equals(node.getNode())) {
                        throw TEST_ERROR;
                    }

                    return COUNTER.incrementAndGet();
                }).get(3, TimeUnit.SECONDS);

                assertFalse(partErrResult.isSuccess());

                nodes.forEach(n -> {
                    if (n.get(ClusterService.class).getTopology().getYoungest().equals(n.getNode())) {
                        assertFalse(partErrResult.isSuccess(n.getNode()));
                        assertNotNull(partErrResult.getError(n.getNode()));
                        assertNotNull(partErrResult.getErrors().get(n.getNode()));

                        assertTrue(partErrResult.getError(n.getNode()).getMessage().contains(TestAssertionError.class.getName()));
                        assertTrue(partErrResult.getError(n.getNode()).getMessage().contains(TEST_ERROR_MESSAGE));
                    } else {
                        assertTrue(partErrResult.isSuccess(n.getNode()));
                        assertNotNull(partErrResult.getResult(n.getNode()));
                    }
                });

                NODES.clear();
                COUNTER.set(0);
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

        MultiNodeResult<Object> result = source.get(TaskService.class).forRemotes().aggregate(() -> {
            try {
                REF.get().leave();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }

            return null;
        }).get(3, TimeUnit.SECONDS);

        assertFalse(result.isSuccess());
        assertEquals(ClosedChannelException.class, result.getError(target.getNode()).getClass());

        source.awaitForStatus(Hekate.State.DOWN);

        target.get(TaskService.class).forNode(target.getNode()).aggregate(() -> {
            REF.set(target);

            return null;
        }).get(3, TimeUnit.SECONDS);

        assertSame(target, REF.get());
    }
}
