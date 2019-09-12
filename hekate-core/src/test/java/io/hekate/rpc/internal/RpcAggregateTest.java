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
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcAggregate;
import io.hekate.rpc.RpcAggregateException;
import io.hekate.rpc.RpcRetry;
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
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcAggregateTest extends RpcServiceTestBase {
    @Rpc
    public interface AggregateRpc {
        @RpcAggregate
        List<Object> list(Object arg);

        @RpcAggregate
        Set<Object> set(Object arg);

        @RpcAggregate
        Map<Object, Object> map(Object arg);

        @RpcAggregate
        Collection<Object> collection(Object arg);

        @RpcAggregate(remoteErrors = IGNORE)
        Collection<Object> ignoreErrors(Object arg);

        @RpcAggregate(remoteErrors = WARN)
        Collection<Object> warnErrors(Object arg);

        @RpcAggregate
        Collection<Object> errors(Object arg);

        @RpcRetry
        @RpcAggregate
        List<Object> retry(Object arg);

        @RpcAggregate
        @RpcRetry(errors = AssertionError.class, maxAttempts = "1", delay = "1", maxDelay = "10")
        List<Object> retryMethod(Object arg);
    }

    private final AggregateRpc rpc1 = mock(AggregateRpc.class);

    private final AggregateRpc rpc2 = mock(AggregateRpc.class);

    private AggregateRpc clientRpc;

    private HekateTestNode client;

    private HekateTestNode server1;

    private HekateTestNode server2;

    public RpcAggregateTest(MultiCodecTestContext ctx) {
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
            when(rpc1.list(i)).thenReturn(asList(i, i, i));
            when(rpc2.list(i)).thenReturn(asList(i, i, i));

            List<Object> result = clientRpc.list(i);

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(i);
            verify(rpc2).list(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testListPartialNull() throws Exception {
        repeat(3, i -> {
            when(rpc1.list(i)).thenReturn(asList(i, i, i));
            when(rpc2.list(i)).thenReturn(null);

            List<Object> result = clientRpc.list(i);

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(i);
            verify(rpc2).list(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSet() throws Exception {
        repeat(3, i -> {
            when(rpc1.set(i)).thenReturn(toSet("a" + i, "b" + i, "c" + i));
            when(rpc2.set(i)).thenReturn(toSet("d" + i, "e" + i, "f" + i));

            Set<Object> result = clientRpc.set(i);

            assertEquals(result, toSet("a" + i, "b" + i, "c" + i, "d" + i, "e" + i, "f" + i));

            verify(rpc1).set(i);
            verify(rpc2).set(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSetPartialNull() throws Exception {
        repeat(3, i -> {
            when(rpc1.set(i)).thenReturn(toSet("a" + i, "b" + i, "c" + i));
            when(rpc2.set(i)).thenReturn(null);

            Set<Object> result = clientRpc.set(i);

            assertEquals(result, toSet("a" + i, "b" + i, "c" + i));

            verify(rpc1).set(i);
            verify(rpc2).set(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testMap() throws Exception {
        repeat(3, i -> {
            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));
            Map<Object, Object> m2 = Stream.of("d" + i, "e" + i, "f" + i).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(i)).thenReturn(m1);
            when(rpc2.map(i)).thenReturn(m2);

            Map<Object, Object> result = clientRpc.map(i);

            Map<Object, Object> expected = new HashMap<>();
            expected.putAll(m1);
            expected.putAll(m2);

            assertEquals(expected, result);

            verify(rpc1).map(i);
            verify(rpc2).map(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testMapPartialNull() throws Exception {
        repeat(3, i -> {
            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(i)).thenReturn(m1);
            when(rpc2.map(i)).thenReturn(null);

            Map<Object, Object> result = clientRpc.map(i);

            assertEquals(m1, result);

            verify(rpc1).map(i);
            verify(rpc2).map(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testCollection() throws Exception {
        repeat(3, i -> {
            when(rpc1.collection(i)).thenReturn(asList(i, i, i));
            when(rpc2.collection(i)).thenReturn(toSet(i));

            Collection<Object> result = clientRpc.collection(i);

            assertEquals(4, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).collection(i);
            verify(rpc2).collection(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testIgnoreError() throws Exception {
        repeat(3, i -> {
            when(rpc1.ignoreErrors(i)).thenThrow(TEST_ERROR);
            when(rpc2.ignoreErrors(i)).thenThrow(TEST_ERROR);

            Collection<Object> result = clientRpc.ignoreErrors(i);

            assertEquals(0, result.size());

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testIgnorePartialError() throws Exception {
        repeat(3, i -> {
            when(rpc1.ignoreErrors(i)).thenReturn(asList(i, i, i));
            when(rpc2.ignoreErrors(i)).thenThrow(TEST_ERROR);

            Collection<Object> result = clientRpc.ignoreErrors(i);

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testWarnPartialError() throws Exception {
        repeat(3, i -> {
            when(rpc1.warnErrors(i)).thenReturn(asList(i, i, i));
            when(rpc2.warnErrors(i)).thenThrow(TEST_ERROR);

            Collection<Object> result = clientRpc.warnErrors(i);

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).warnErrors(i);
            verify(rpc2).warnErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testError() throws Exception {
        repeat(3, i -> {
            when(rpc1.errors(i)).thenThrow(TEST_ERROR);
            when(rpc2.errors(i)).thenThrow(TEST_ERROR);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> clientRpc.errors(i));

            assertTrue(err.partialResults().isEmpty());

            assertEquals(2, err.errors().size());
            assertEquals(TEST_ERROR.getClass(), err.errors().get(server2.localNode()).getClass());
            assertEquals(TEST_ERROR.getClass(), err.errors().get(server2.localNode()).getClass());

            verify(rpc1).errors(i);
            verify(rpc2).errors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testPartialError() throws Exception {
        repeat(3, i -> {
            when(rpc1.errors(i)).thenReturn(asList(i, i, i));
            when(rpc2.errors(i)).thenThrow(TEST_ERROR);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> clientRpc.errors(i));

            assertEquals(1, err.partialResults().size());
            assertEquals(asList(i, i, i), err.partialResults().get(server1.localNode()));

            assertEquals(1, err.errors().size());
            assertEquals(TEST_ERROR.getClass(), err.errors().get(server2.localNode()).getClass());

            verify(rpc1).errors(i);
            verify(rpc2).errors(i);

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
            when(rpc1.list(i)).thenAnswer(invocation -> nestedRpc1.list(i));
            when(rpc2.list(i)).thenReturn(asList(i, i, i));

            List<Object> result = clientRpc.list(i);

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(i);
            verify(rpc2, times(2)).list(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testRecursiveError() throws Exception {
        AggregateRpc nestedRpc1 = server1.rpc().clientFor(AggregateRpc.class)
            .withTimeout(1, TimeUnit.SECONDS)
            .forRemotes()
            .build();

        repeat(3, i -> {
            when(rpc1.errors(i)).thenAnswer(invocation -> nestedRpc1.errors(i));
            when(rpc2.errors(i)).thenThrow(TEST_ERROR);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> clientRpc.errors(i));

            assertEquals(2, err.errors().size());
            assertEquals(TEST_ERROR.getClass(), err.errors().get(server2.localNode()).getClass());

            RpcAggregateException rpc1Err = (RpcAggregateException)err.errors().get(server1.localNode());

            assertEquals(1, rpc1Err.errors().size());
            assertEquals(TEST_ERROR.getClass(), rpc1Err.errors().get(server2.localNode()).getClass());

            verify(rpc1).errors(i);
            verify(rpc2, times(2)).errors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
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

            List<Object> result = clientRpc.retry("test");

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

            List<Object> result = clientRpc.retryMethod("test");

            verify(rpc1).retryMethod(any());
            verify(rpc2, times(2)).retryMethod(any());

            assertEquals(asList("ok", "ok"), result);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }
}
