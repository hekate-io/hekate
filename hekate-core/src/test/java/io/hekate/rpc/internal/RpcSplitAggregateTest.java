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

package io.hekate.rpc.internal;

import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcAggregate;
import io.hekate.rpc.RpcException;
import io.hekate.rpc.RpcRetry;
import io.hekate.rpc.RpcSplit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Test;

import static io.hekate.rpc.RpcAggregate.RemoteErrors.IGNORE;
import static io.hekate.rpc.RpcAggregate.RemoteErrors.WARN;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcSplitAggregateTest extends RpcServiceTestBase {
    @Rpc
    public interface AggregateRpc {
        @RpcAggregate
        List<Object> list(@RpcSplit List<Object> arg);

        @RpcAggregate
        Set<Object> set(@RpcSplit Set<Object> arg);

        @RpcAggregate
        Map<Object, Object> map(@RpcSplit Map<Object, Object> arg);

        @RpcAggregate
        Collection<Object> collection(@RpcSplit Collection<Object> arg);

        @RpcAggregate(remoteErrors = IGNORE)
        Collection<Object> ignoreErrors(@RpcSplit List<Object> arg);

        @RpcAggregate(remoteErrors = WARN)
        Collection<Object> warnErrors(@RpcSplit List<Object> arg);

        @RpcAggregate
        Collection<Object> errors(@RpcSplit List<Object> arg);

        @RpcRetry
        @RpcAggregate
        List<Object> retry(@RpcSplit List<Object> arg);

        @RpcAggregate
        @RpcRetry(maxAttempts = "1", delay = "10", maxDelay = "100")
        List<Object> retryMethod(@RpcSplit List<Object> arg);
    }

    private final AggregateRpc rpc1 = mock(AggregateRpc.class);

    private final AggregateRpc rpc2 = mock(AggregateRpc.class);

    private AggregateRpc clientRpc;

    private HekateTestNode server1;

    private HekateTestNode server2;

    private HekateTestNode client;

    public RpcSplitAggregateTest(MultiCodecTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ClientAndServers ctx = prepareClientAndServers(rpc1, rpc2);

        server1 = ctx.servers().get(0);
        server2 = ctx.servers().get(1);

        client = ctx.client();

        clientRpc = client.rpc().clientFor(AggregateRpc.class)
            .withTimeout(AWAIT_TIMEOUT, TimeUnit.SECONDS)
            .build();
    }

    @Test
    public void testList() throws Exception {
        repeat(3, i -> {
            when(rpc1.list(singletonList(i))).thenReturn(asList(i, i, i));
            when(rpc2.list(singletonList(i))).thenReturn(asList(i, i, i));

            List<Object> result = clientRpc.list(asList(i, i));

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(singletonList(i));
            verify(rpc2).list(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSmallList() throws Exception {
        repeat(3, i -> {
            when(rpc1.list(anyList())).thenReturn(singletonList(i));
            when(rpc2.list(anyList())).thenReturn(singletonList(i));

            List<Object> result = clientRpc.list(singletonList(i));

            assertEquals(1, result.size());

            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testLargerList() throws Exception {
        repeat(3, i -> {
            when(rpc1.list(anyList())).thenReturn(asList(i, i, i));
            when(rpc2.list(anyList())).thenReturn(asList(i, i, i));

            List<Object> result = clientRpc.list(asList(i + 1, i + 2, i + 3, i + 4));

            assertEquals(12, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1, times(2)).list(anyList());
            verify(rpc2, times(2)).list(anyList());

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testListPartialNull() throws Exception {
        repeat(3, i -> {
            when(rpc1.list(singletonList(i))).thenReturn(asList(i, i, i));
            when(rpc2.list(singletonList(i))).thenReturn(null);

            List<Object> result = clientRpc.list(asList(i, i));

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(singletonList(i));
            verify(rpc2).list(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSet() throws Exception {
        repeat(3, i -> {
            when(rpc1.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(toSet("a" + i, "b" + i, "c" + i));
            when(rpc2.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(toSet("d" + i, "e" + i, "f" + i));

            Set<Object> result = clientRpc.set(toSet(i + 1, i + 2));

            assertEquals(result, toSet("a" + i, "b" + i, "c" + i, "d" + i, "e" + i, "f" + i));

            verify(rpc1).set(or(eq(singleton(i + 1)), eq(singleton(i + 2))));
            verify(rpc2).set(or(eq(singleton(i + 1)), eq(singleton(i + 2))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSmallSet() throws Exception {
        repeat(3, i -> {
            when(rpc1.set(anySet())).thenReturn(toSet("a" + i));
            when(rpc2.set(anySet())).thenReturn(toSet("b" + i));

            Set<Object> result = clientRpc.set(toSet(i));

            assertEquals(1, result.size());

            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testLargerSet() throws Exception {
        repeat(3, i -> {
            when(rpc1.set(anySet())).thenAnswer(invocation -> invocation.getArgument(0));
            when(rpc2.set(anySet())).thenAnswer(invocation -> invocation.getArgument(0));

            Set<Object> result = clientRpc.set(toSet(i + 1, i + 2, i + 3, i + 4));

            assertEquals(result, toSet(i + 1, i + 2, i + 3, i + 4));

            verify(rpc1, times(2)).set(anySet());
            verify(rpc2, times(2)).set(anySet());

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSetPartialNull() throws Exception {
        repeat(3, i -> {
            when(rpc1.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(toSet("a" + i, "b" + i, "c" + i));
            when(rpc2.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(null);

            Set<Object> result = clientRpc.set(toSet(i + 1, i + 2));

            assertEquals(result, toSet("a" + i, "b" + i, "c" + i));

            verify(rpc1).set(or(eq(singleton(i + 1)), eq(singleton(i + 2))));
            verify(rpc2).set(or(eq(singleton(i + 1)), eq(singleton(i + 2))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testMap() throws Exception {
        repeat(3, i -> {
            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));
            Map<Object, Object> m2 = Stream.of("d" + i, "e" + i, "f" + i).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(m1);
            when(rpc2.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(m2);

            Map<Object, Object> args = new HashMap<>();
            args.put(i + 1, i);
            args.put(i + 2, i);

            Map<Object, Object> result = clientRpc.map(args);

            Map<Object, Object> expected = new HashMap<>();
            expected.putAll(m1);
            expected.putAll(m2);

            assertEquals(expected, result);

            verify(rpc1).map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))));
            verify(rpc2).map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSmallerMap() throws Exception {
        repeat(3, i -> {
            when(rpc1.map(anyMap())).thenReturn(singletonMap("a", i));
            when(rpc2.map(anyMap())).thenReturn(singletonMap("b", i));

            Map<Object, Object> result = clientRpc.map(singletonMap("d", i));

            assertEquals(1, result.size());

            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testLargerMap() throws Exception {
        repeat(3, i -> {

            when(rpc1.map(anyMap())).thenAnswer(invocation -> invocation.getArgument(0));
            when(rpc2.map(anyMap())).thenAnswer(invocation -> invocation.getArgument(0));

            Map<Object, Object> args = Stream.of(i + 1, i + 2, i + 3, i + 4).collect(toMap(o -> o, o -> o + "test"));

            Map<Object, Object> result = clientRpc.map(args);

            Map<Object, Object> expected = new HashMap<>(args);

            assertEquals(expected, result);

            verify(rpc1, times(2)).map(anyMap());
            verify(rpc2, times(2)).map(anyMap());

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testMapPartialNull() throws Exception {
        repeat(3, i -> {
            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(m1);
            when(rpc2.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(null);

            Map<Object, Object> args = new HashMap<>();
            args.put(i + 1, i);
            args.put(i + 2, i);

            Map<Object, Object> result = clientRpc.map(args);

            assertEquals(m1, result);

            verify(rpc1).map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))));
            verify(rpc2).map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testCollection() throws Exception {
        repeat(3, i -> {
            when(rpc1.collection(singletonList(i))).thenReturn(asList(i, i, i));
            when(rpc2.collection(singletonList(i))).thenReturn(toSet(i));

            Collection<Object> result = clientRpc.collection(asList(i, i));

            assertEquals(4, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).collection(singletonList(i));
            verify(rpc2).collection(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testIgnoreError() throws Exception {
        repeat(3, i -> {
            when(rpc1.ignoreErrors(singletonList(i))).thenThrow(TEST_ERROR);
            when(rpc2.ignoreErrors(singletonList(i))).thenThrow(TEST_ERROR);

            Collection<Object> result = clientRpc.ignoreErrors(asList(i, i));

            assertEquals(0, result.size());

            verify(rpc1).ignoreErrors(singletonList(i));
            verify(rpc2).ignoreErrors(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testIgnorePartialError() throws Exception {
        repeat(3, i -> {
            when(rpc1.ignoreErrors(singletonList(i))).thenReturn(asList(i, i, i));
            when(rpc2.ignoreErrors(singletonList(i))).thenThrow(TEST_ERROR);

            Collection<Object> result = clientRpc.ignoreErrors(asList(i, i));

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).ignoreErrors(singletonList(i));
            verify(rpc2).ignoreErrors(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testWarnPartialError() throws Exception {
        repeat(3, i -> {
            when(rpc1.warnErrors(singletonList(i))).thenReturn(asList(i, i, i));
            when(rpc2.warnErrors(singletonList(i))).thenThrow(TEST_ERROR);

            Collection<Object> result = clientRpc.warnErrors(asList(i, i));

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).warnErrors(singletonList(i));
            verify(rpc2).warnErrors(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testError() throws Exception {
        repeat(3, i -> {
            when(rpc1.errors(singletonList(i))).thenThrow(TEST_ERROR);
            when(rpc2.errors(singletonList(i))).thenThrow(TEST_ERROR);

            expect(RpcException.class, () -> clientRpc.errors(asList(i, i)));

            verify(rpc1).errors(singletonList(i));
            verify(rpc2).errors(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testPartialError() throws Exception {
        repeat(3, i -> {
            when(rpc1.errors(singletonList(i))).thenReturn(asList(i, i, i));
            when(rpc2.errors(singletonList(i))).thenThrow(TEST_ERROR);

            expect(RpcException.class, () -> clientRpc.errors(asList(i, i)));

            verify(rpc1).errors(singletonList(i));
            verify(rpc2).errors(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testRecursiveResult() throws Exception {
        AggregateRpc nestedRpc1 = server1.rpc().clientFor(AggregateRpc.class)
            .withTimeout(1, TimeUnit.SECONDS)
            .forRemotes()
            .build();

        repeat(3, i -> {
            when(rpc1.list(singletonList(i))).thenAnswer(invocation -> nestedRpc1.list(singletonList(i)));
            when(rpc2.list(singletonList(i))).thenReturn(asList(i, i, i));

            List<Object> result = clientRpc.list(asList(i, i));

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(singletonList(i));
            verify(rpc2, times(2)).list(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testEmptyTopology() throws Exception {
        server1.leave();
        server2.leave();

        RpcException err = expect(RpcException.class, () -> clientRpc.list(singletonList(1)));

        assertTrue(err.isCausedBy(EmptyTopologyException.class));
    }

    @Test
    public void testRetryDefaultSettings() throws Exception {
        clientRpc = client.rpc().clientFor(AggregateRpc.class)
            .withRetryPolicy(retry -> retry
                .withFixedDelay(10)
                .maxAttempts(1)
            )
            .build();

        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            when(rpc1.retry(any())).thenReturn(singletonList("ok"));
            when(rpc2.retry(any())).thenAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return singletonList("ok");
                }
            });

            List<Object> result = clientRpc.retry(asList("test", "test"));

            verify(rpc1).retry(any());
            verify(rpc2, times(2)).retry(any());

            assertEquals(asList("ok", "ok"), result);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testRetryMethodSettings() throws Exception {
        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            when(rpc1.retryMethod(any())).thenReturn(singletonList("ok"));
            when(rpc2.retryMethod(any())).thenAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return singletonList("ok");
                }
            });

            List<Object> result = clientRpc.retryMethod(asList("test", "test"));

            verify(rpc1).retryMethod(any());
            verify(rpc2, times(2)).retryMethod(any());

            assertEquals(asList("ok", "ok"), result);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }
}
