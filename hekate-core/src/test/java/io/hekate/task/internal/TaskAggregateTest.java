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
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessagingException;
import io.hekate.task.MultiNodeResult;
import io.hekate.task.RemoteTaskException;
import java.nio.channels.ClosedChannelException;
import java.util.List;
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
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                MultiNodeResult<Integer> result = get(node.tasks().aggregate(() -> {
                    int intResult = COUNTER.incrementAndGet();

                    NODES.add(node.localNode());

                    return intResult;
                }));

                List<Integer> sorted = result.stream().sorted().collect(toList());

                assertEquals(nodes.size(), COUNTER.get());
                assertTrue(NODES.toString(), NODES.containsAll(nodes.stream().map(Hekate::localNode).collect(toList())));
                assertTrue(result.isSuccess());
                nodes.forEach(n -> assertTrue(result.isSuccess(n.localNode())));
                assertTrue(result.nodes().containsAll(nodes.stream().map(Hekate::localNode).collect(toList())));

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
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                MultiNodeResult<Integer> affResult = get(node.tasks().withAffinity(100500).aggregate(() -> {
                    int intResult = COUNTER.incrementAndGet();

                    NODES.add(node.localNode());

                    return intResult;
                }));

                List<Integer> affSorted = affResult.stream().sorted().collect(toList());

                assertEquals(nodes.size(), COUNTER.get());
                assertTrue(NODES.toString(), NODES.containsAll(nodes.stream().map(Hekate::localNode).collect(toList())));
                assertTrue(affResult.isSuccess());
                nodes.forEach(n -> assertTrue(affResult.isSuccess(n.localNode())));
                assertTrue(affResult.nodes().containsAll(nodes.stream().map(Hekate::localNode).collect(toList())));

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
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                MultiNodeResult<?> errResult = get(node.tasks().aggregate(() -> {
                    throw TEST_ERROR;
                }));

                assertFalse(errResult.isSuccess());
                assertTrue(errResult.results().isEmpty());
                nodes.forEach(n -> {
                    assertFalse(errResult.isSuccess(n.localNode()));
                    Throwable err = errResult.errorOf(n.localNode());

                    assertNotNull(err);
                    assertSame(RemoteTaskException.class, err.getClass());

                    RemoteTaskException taskErr = (RemoteTaskException)err;

                    assertEquals(n.localNode().id(), taskErr.remoteNodeId());
                    assertEquals(TEST_ERROR.getClass(), taskErr.getCause().getClass());
                    assertEquals(TEST_ERROR.getMessage(), taskErr.getCause().getMessage());
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
            List<HekateTestNode> nodes = createAndJoin(i + 1);

            for (HekateTestNode node : nodes) {
                MultiNodeResult<Integer> partErrResult = get(node.tasks().aggregate(() -> {
                    if (node.cluster().topology().youngest().equals(node.localNode())) {
                        throw TEST_ERROR;
                    }

                    return COUNTER.incrementAndGet();
                }));

                assertFalse(partErrResult.isSuccess());

                nodes.forEach(n -> {
                    if (n.cluster().topology().youngest().equals(n.localNode())) {
                        assertFalse(partErrResult.isSuccess(n.localNode()));

                        RemoteTaskException err = (RemoteTaskException)partErrResult.errorOf(n.localNode());

                        assertNotNull(err);
                        assertSame(err, partErrResult.errors().get(n.localNode()));

                        assertEquals(TEST_ERROR.getClass(), err.getCause().getClass());
                    } else {
                        assertTrue(partErrResult.isSuccess(n.localNode()));
                        assertNotNull(partErrResult.resultOf(n.localNode()));
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
        List<HekateTestNode> nodes = createAndJoin(2);

        HekateTestNode source = nodes.get(0);
        HekateTestNode target = nodes.get(1);

        REF.set(source);

        MultiNodeResult<Object> result = get(source.tasks().forRemotes().aggregate(() -> {
            try {
                REF.get().leave();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }

            return null;
        }));

        assertFalse(result.isSuccess());

        Throwable error = result.errorOf(target.localNode());

        assertSame(MessagingException.class, error.getClass());
        assertTrue(ErrorUtils.stackTrace(error), ErrorUtils.isCausedBy(ClosedChannelException.class, error));

        source.awaitForStatus(Hekate.State.DOWN);

        get(target.tasks().forNode(target.localNode()).aggregate(() -> {
            REF.set(target);

            return null;
        }));

        assertSame(target, REF.get());
    }
}
