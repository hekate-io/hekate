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

package io.hekate.coordinate.internal;

import io.hekate.HekateInstanceContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.coordinate.CoordinationContext;
import io.hekate.coordinate.CoordinationFuture;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationRequest;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.core.HekateTestInstance;
import io.hekate.core.LeaveFuture;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Test;
import org.mockito.InOrder;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class CoordinationServiceTest extends HekateInstanceContextTestBase {
    private static class CoordinatedValueHandler implements CoordinationHandler {
        private volatile String lastValue;

        @Override
        public void prepare(CoordinationContext ctx) {
            if (lastValue == null) {
                lastValue = "0";
            }
        }

        @Override
        public void coordinate(CoordinationContext ctx) {
            ctx.broadcast("get", getResponses -> {
                Optional<Integer> maxOpt = getResponses.values().stream()
                    .map(String.class::cast)
                    .map(Integer::parseInt)
                    .max(Integer::compare);

                assertTrue(maxOpt.isPresent());

                int newMax = maxOpt.get() + 1;

                ctx.broadcast("put_" + newMax, putResponses -> {
                    // No-op.
                });
            });
        }

        @Override
        public void process(CoordinationRequest request, CoordinationContext ctx) {
            String command = request.get();

            if ("get".equals(command)) {
                request.reply(lastValue);
            } else {
                lastValue = command.substring("put_".length());

                request.reply("ok");

                ctx.complete();
            }
        }

        @Override
        public void initialize() {
            lastValue = null;
        }

        @Override
        public void terminate() {
            lastValue = null;
        }

        public String getLastValue() {
            return lastValue;
        }
    }

    public CoordinationServiceTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testHandlerLifecycleWithCompletedProcess() throws Exception {
        CoordinationHandler handler = mock(CoordinationHandler.class);

        HekateTestInstance node = createInstanceWithCoordination(handler);

        repeat(3, i -> {
            doAnswer(invocation -> {
                ((CoordinationContext)invocation.getArguments()[0]).complete();

                return null;
            }).when(handler).coordinate(any());

            node.join();

            node.get(CoordinationService.class).futureOf("test").get(3, TimeUnit.SECONDS);

            InOrder order = inOrder(handler);

            order.verify(handler).initialize();
            order.verify(handler).prepare(any());
            order.verify(handler).coordinate(any());

            verifyNoMoreInteractions(handler);

            node.leave();

            order.verify(handler).terminate();

            verifyNoMoreInteractions(handler);

            reset(handler);
        });
    }

    @Test
    public void testHandlerLifecycleWithNonCompletedProcess() throws Exception {
        CoordinationHandler handler = mock(CoordinationHandler.class);

        HekateTestInstance node = createInstanceWithCoordination(handler);

        repeat(3, i -> {
            CountDownLatch coordinateLatch = new CountDownLatch(1);

            doAnswer(invocation -> {
                coordinateLatch.countDown();

                return null;
            }).when(handler).coordinate(any());

            node.join();

            await(coordinateLatch);

            InOrder order = inOrder(handler);

            order.verify(handler).initialize();
            order.verify(handler).prepare(any());
            order.verify(handler).coordinate(any());

            verifyNoMoreInteractions(handler);

            node.leave();

            order.verify(handler).cancel(any());
            order.verify(handler).terminate();

            verifyNoMoreInteractions(handler);

            reset(handler);
        });
    }

    @Test
    public void testCoordinationFuture() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(1);

        CoordinationHandler handler = mock(CoordinationHandler.class);

        doAnswer(invocation -> {
            startLatch.countDown();

            endLatch.await(3, TimeUnit.SECONDS);

            ((CoordinationContext)invocation.getArguments()[0]).complete();

            return null;
        }).when(handler).coordinate(any());

        HekateTestInstance node = createInstanceWithCoordination(handler);

        node.join();

        CoordinationFuture future = node.get(CoordinationService.class).futureOf("test");

        await(startLatch);

        assertFalse(future.isDone());

        endLatch.countDown();

        future.get(3, TimeUnit.SECONDS);

        assertTrue(future.isDone());
        assertNotNull(future.getNow(null));
        assertSame(handler, future.getNow(null).getHandler());
    }

    @Test
    public void testCoordinationWithLeaveBeforeCompletion() throws Exception {
        CountDownLatch startLatch = new CountDownLatch(1);

        CoordinationHandler handler = mock(CoordinationHandler.class);

        doAnswer(invocation -> {
            startLatch.countDown();

            return null;
        }).when(handler).coordinate(any());

        HekateTestInstance node = createInstanceWithCoordination(handler);

        node.join();

        CoordinationFuture future = node.get(CoordinationService.class).futureOf("test");

        await(startLatch);

        assertFalse(future.isDone());

        node.leave();

        expect(CancellationException.class, () -> future.get(3, TimeUnit.SECONDS));

        assertTrue(future.isDone());
        assertTrue(future.isCancelled());
    }

    @Test
    public void testCoordinatedValue() throws Exception {
        repeat(5, i -> {
            int nodesCount = i + 1;

            List<HekateTestInstance> nodes = new ArrayList<>(nodesCount);

            int val = 0;

            for (int j = 0; j < nodesCount; j++) {
                HekateTestInstance node = createInstanceWithCoordination(new CoordinatedValueHandler()).join();

                nodes.add(node);

                node.get(CoordinationService.class).futureOf("test").get(3, TimeUnit.SECONDS);

                String expected = String.valueOf(++val);

                awaitForCoordinatedValue(expected, nodes);
            }

            for (Iterator<HekateTestInstance> it = nodes.iterator(); it.hasNext(); ) {
                HekateTestInstance node = it.next();

                it.remove();

                node.leave();

                String expected = String.valueOf(++val);

                if (it.hasNext()) {
                    awaitForCoordinatedValue(expected, nodes);
                }
            }
        });
    }

    @Test
    public void testCoordinatorLeave() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        AtomicReference<ClusterNode> coordinatorRef = new AtomicReference<>();
        AtomicInteger aborts = new AtomicInteger();

        class BlockingHandler extends CoordinatedValueHandler {
            @Override
            public void coordinate(CoordinationContext ctx) {
                if (ctx.getSize() == 3 && blockLatch.getCount() > 0) {
                    coordinatorRef.set(ctx.getCoordinator().getNode());

                    blockLatch.countDown();

                    await(proceedLatch);
                } else {
                    super.coordinate(ctx);
                }
            }

            @Override
            public void cancel(CoordinationContext ctx) {
                super.cancel(ctx);

                aborts.incrementAndGet();
            }
        }

        BlockingHandler h1 = new BlockingHandler();
        BlockingHandler h2 = new BlockingHandler();
        BlockingHandler h3 = new BlockingHandler();

        HekateTestInstance n1 = createInstanceWithCoordination(h1);
        HekateTestInstance n2 = createInstanceWithCoordination(h2);
        HekateTestInstance n3 = createInstanceWithCoordination(h3);

        assertEquals("1", n1.join().get(CoordinationService.class).futureOf("test").get().<BlockingHandler>getHandler().getLastValue());
        assertEquals("2", n2.join().get(CoordinationService.class).futureOf("test").get().<BlockingHandler>getHandler().getLastValue());

        n3.join(); // <-- Coordination should block.

        await(blockLatch);

        HekateTestInstance coordinator = Stream.of(n1, n2, n3)
            .filter(n -> n.getNode().equals(coordinatorRef.get()))
            .findFirst().orElseThrow(AssertionError::new);

        LeaveFuture leaveFuture = coordinator.leaveAsync();

        proceedLatch.countDown();

        leaveFuture.get(3, TimeUnit.SECONDS);

        List<HekateTestInstance> remaining = Stream.of(n1, n2, n3).filter(n -> !n.equals(coordinator)).collect(toList());

        awaitForCoordinatedValue("3", remaining);

        assertEquals(3, aborts.get());
    }

    @Test
    public void testJoinDuringCoordination() throws Exception {
        CountDownLatch blockLatch = new CountDownLatch(1);
        CountDownLatch proceedLatch = new CountDownLatch(1);

        AtomicInteger aborts = new AtomicInteger();

        class BlockingHandler extends CoordinatedValueHandler {
            @Override
            public void coordinate(CoordinationContext ctx) {
                if (ctx.getSize() == 2 && blockLatch.getCount() > 0) {
                    blockLatch.countDown();

                    await(proceedLatch);
                }

                super.coordinate(ctx);
            }

            @Override
            public void cancel(CoordinationContext ctx) {
                super.cancel(ctx);

                aborts.incrementAndGet();
            }
        }

        BlockingHandler h1 = new BlockingHandler();
        BlockingHandler h2 = new BlockingHandler();
        BlockingHandler h3 = new BlockingHandler();

        HekateTestInstance n1 = createInstanceWithCoordination(h1);
        HekateTestInstance n2 = createInstanceWithCoordination(h2);
        HekateTestInstance n3 = createInstanceWithCoordination(h3);

        // Join and await for initial coordination on the first node.
        n1.join().get(CoordinationService.class).futureOf("test").get(3, TimeUnit.SECONDS);

        n2.join(); // <-- Coordination should block.

        await(blockLatch);

        n3.join();

        awaitForTopology(n1, n2, n3);

        proceedLatch.countDown();

        // Expect 2 but not 3 since one coordination should be interrupted by a concurrent join.
        awaitForCoordinatedValue("2", Arrays.asList(n1, n2, n3));

        assertEquals(2, aborts.get());
    }

    private void awaitForCoordinatedValue(String expected, List<HekateTestInstance> nodes) throws Exception {
        busyWait("value coordination", () ->
            nodes.stream().allMatch(n -> {
                CoordinatedValueHandler handler = n.get(CoordinationService.class).process("test").getHandler();

                return handler.getLastValue().equals(expected);
            })
        );
    }

    private HekateTestInstance createInstanceWithCoordination(CoordinationHandler handler) throws Exception {
        return createInstance(c ->
            c.withService(new CoordinationServiceFactory()
                .withProcess(new CoordinationProcessConfig()
                    .withName("test")
                    .withHandler(handler)
                )
            )
        );
    }
}
