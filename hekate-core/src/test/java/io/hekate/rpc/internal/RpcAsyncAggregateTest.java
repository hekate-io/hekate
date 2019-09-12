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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcAsyncAggregateTest extends RpcServiceTestBase {
    @Rpc
    public interface AggregateRpc {
        @RpcAggregate
        CompletableFuture<List<Object>> list(Object arg);

        @RpcAggregate
        CompletableFuture<Set<Object>> set(Object arg);

        @RpcAggregate
        CompletableFuture<Map<Object, Object>> map(Object arg);

        @RpcAggregate
        CompletableFuture<Collection<Object>> collection(Object arg);

        @RpcAggregate(remoteErrors = RpcAggregate.RemoteErrors.IGNORE)
        CompletableFuture<Collection<Object>> ignoreErrors(Object arg);

        @RpcAggregate
        CompletableFuture<Collection<Object>> errors(Object arg);
    }

    private final AggregateRpc rpc1 = mock(AggregateRpc.class);

    private final AggregateRpc rpc2 = mock(AggregateRpc.class);

    private AggregateRpc client;

    private HekateTestNode server1;

    private HekateTestNode server2;

    public RpcAsyncAggregateTest(MultiCodecTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ClientAndServers ctx = prepareClientAndServers(rpc1, rpc2);

        server1 = ctx.servers().get(0);
        server2 = ctx.servers().get(1);

        client = ctx.client().rpc().clientFor(AggregateRpc.class)
            .withTimeout(AWAIT_TIMEOUT, TimeUnit.SECONDS)
            .build();
    }

    @Test
    public void testAsyncList() throws Exception {
        repeat(3, i -> {
            CompletableFuture<List<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<List<Object>> f2 = new CompletableFuture<>();

            when(rpc1.list(i)).thenReturn(f1);
            when(rpc2.list(i)).thenReturn(f2);

            CompletableFuture<List<Object>> future = client.list(i);

            assertFalse(future.isDone());

            f1.complete(asList(i, i, i));
            f2.complete(asList(i, i, i));

            List<Object> result = get(future);

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(i);
            verify(rpc2).list(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncListPartialNull() throws Exception {
        repeat(3, i -> {
            CompletableFuture<List<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<List<Object>> f2 = new CompletableFuture<>();

            when(rpc1.list(i)).thenReturn(f1);
            when(rpc2.list(i)).thenReturn(f2);

            CompletableFuture<List<Object>> future = client.list(i);

            assertFalse(future.isDone());

            f1.complete(asList(i, i, i));
            f2.complete(null);

            List<Object> result = get(future);

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(i);
            verify(rpc2).list(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncSet() throws Exception {
        repeat(3, i -> {
            Set<Object> s1 = toSet("a" + i, "b" + i, "c" + i);
            Set<Object> s2 = toSet("d" + i, "e" + i, "f" + i);

            CompletableFuture<Set<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Set<Object>> f2 = new CompletableFuture<>();

            when(rpc1.set(i)).thenReturn(f1);
            when(rpc2.set(i)).thenReturn(f2);

            CompletableFuture<Set<Object>> future = client.set(i);

            assertFalse(future.isDone());

            f1.complete(s1);
            f2.complete(s2);

            Set<Object> result = get(future);

            Set<Object> expected = new HashSet<>();

            expected.addAll(s1);
            expected.addAll(s2);

            assertEquals(expected, result);

            verify(rpc1).set(i);
            verify(rpc2).set(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncSetPartialNull() throws Exception {
        repeat(3, i -> {
            Set<Object> s1 = toSet("a" + i, "b" + i, "c" + i);

            CompletableFuture<Set<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Set<Object>> f2 = new CompletableFuture<>();

            when(rpc1.set(i)).thenReturn(f1);
            when(rpc2.set(i)).thenReturn(f2);

            CompletableFuture<Set<Object>> future = client.set(i);

            assertFalse(future.isDone());

            f1.complete(s1);
            f2.complete(null);

            Set<Object> result = get(future);

            Set<Object> expected = new HashSet<>(s1);

            assertEquals(expected, result);

            verify(rpc1).set(i);
            verify(rpc2).set(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncMap() throws Exception {
        repeat(3, i -> {
            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));
            Map<Object, Object> m2 = Stream.of("d" + i, "e" + i, "f" + i).collect(toMap(o -> o, o -> o + "test"));

            CompletableFuture<Map<Object, Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Map<Object, Object>> f2 = new CompletableFuture<>();

            when(rpc1.map(i)).thenReturn(f1);
            when(rpc2.map(i)).thenReturn(f2);

            CompletableFuture<Map<Object, Object>> future = client.map(i);

            assertFalse(future.isDone());

            f1.complete(m1);
            f2.complete(m2);

            Map<Object, Object> result = get(future);

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
    public void testAsyncMapPartialNull() throws Exception {
        repeat(3, i -> {
            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));

            CompletableFuture<Map<Object, Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Map<Object, Object>> f2 = new CompletableFuture<>();

            when(rpc1.map(i)).thenReturn(f1);
            when(rpc2.map(i)).thenReturn(f2);

            CompletableFuture<Map<Object, Object>> future = client.map(i);

            assertFalse(future.isDone());

            f1.complete(m1);
            f2.complete(null);

            Map<Object, Object> result = get(future);

            Map<Object, Object> expected = new HashMap<>(m1);

            assertEquals(expected, result);

            verify(rpc1).map(i);
            verify(rpc2).map(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncCollection() throws Exception {
        repeat(3, i -> {
            List<Object> c1 = asList(i, i, i);
            Set<Object> c2 = toSet(i);

            CompletableFuture<Collection<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Collection<Object>> f2 = new CompletableFuture<>();

            when(rpc1.collection(i)).thenReturn(f1);
            when(rpc2.collection(i)).thenReturn(f2);

            CompletableFuture<Collection<Object>> future = client.collection(i);

            assertFalse(future.isDone());

            f1.complete(c1);
            f2.complete(c2);

            Collection<Object> result = get(future);

            assertEquals(4, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).collection(i);
            verify(rpc2).collection(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncIgnoreError() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Collection<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Collection<Object>> f2 = new CompletableFuture<>();

            when(rpc1.ignoreErrors(i)).thenReturn(f1);
            when(rpc2.ignoreErrors(i)).thenReturn(f2);

            CompletableFuture<Collection<Object>> future = client.ignoreErrors(i);

            assertFalse(future.isDone());

            f1.completeExceptionally(TEST_ERROR);
            f2.completeExceptionally(TEST_ERROR);

            Collection<Object> result = get(future);

            assertEquals(0, result.size());

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncIgnorePartialError() throws Exception {
        repeat(3, i -> {
            List<Object> v1 = asList(i, i, i);

            CompletableFuture<Collection<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Collection<Object>> f2 = new CompletableFuture<>();

            when(rpc1.ignoreErrors(i)).thenReturn(f1);
            when(rpc2.ignoreErrors(i)).thenReturn(f2);

            CompletableFuture<Collection<Object>> future = client.ignoreErrors(i);

            assertFalse(future.isDone());

            f1.complete(v1);
            f2.completeExceptionally(TEST_ERROR);

            Collection<Object> result = get(future);

            assertEquals(3, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncPartialError() throws Exception {
        repeat(3, i -> {
            List<Object> v1 = asList(i, i, i);

            CompletableFuture<Collection<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Collection<Object>> f2 = new CompletableFuture<>();

            when(rpc1.errors(i)).thenReturn(f1);
            when(rpc2.errors(i)).thenReturn(f2);

            CompletableFuture<Collection<Object>> future = client.errors(i);

            assertFalse(future.isDone());

            f1.complete(v1);
            f2.completeExceptionally(TEST_ERROR);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> {
                try {
                    get(future);
                } catch (ExecutionException e) {
                    throw (Exception)e.getCause();
                }
            });

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
    public void testAsyncError() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Collection<Object>> f1 = new CompletableFuture<>();
            CompletableFuture<Collection<Object>> f2 = new CompletableFuture<>();

            when(rpc1.errors(i)).thenReturn(f1);
            when(rpc2.errors(i)).thenReturn(f2);

            CompletableFuture<Collection<Object>> future = client.errors(i);

            assertFalse(future.isDone());

            f1.completeExceptionally(TEST_ERROR);
            f2.completeExceptionally(TEST_ERROR);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> {
                try {
                    get(future);
                } catch (ExecutionException e) {
                    throw (Exception)e.getCause();
                }
            });

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
    public void testAsyncRecursiveResult() throws Exception {
        AggregateRpc nestedRpc1 = server1.rpc().clientFor(AggregateRpc.class)
            .withTimeout(1, TimeUnit.SECONDS)
            .forRemotes()
            .build();

        repeat(3, i -> {
            List<CompletableFuture<Collection<Object>>> rpc2responses = Collections.synchronizedList(new ArrayList<>());

            when(rpc1.list(i)).thenAnswer(invocation -> nestedRpc1.list(i));
            when(rpc2.list(i)).thenAnswer(invocation -> {
                CompletableFuture<Collection<Object>> future = new CompletableFuture<>();

                rpc2responses.add(future);

                return future;
            });

            CompletableFuture<List<Object>> future = client.list(i);

            assertFalse(future.isDone());

            busyWait("RPC2 to get all requests.", () -> rpc2responses.size() == 2);

            rpc2responses.forEach(f -> f.complete(Arrays.asList(i, i, i)));

            List<Object> result = get(future);

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(i);
            verify(rpc2, times(2)).list(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncRecursiveError() throws Exception {
        AggregateRpc nestedRpc1 = server1.rpc().clientFor(AggregateRpc.class)
            .withTimeout(1, TimeUnit.SECONDS)
            .forRemotes()
            .build();

        repeat(3, i -> {
            List<CompletableFuture<Collection<Object>>> rpc2responses = Collections.synchronizedList(new ArrayList<>());

            when(rpc1.errors(i)).thenAnswer(invocation -> CompletableFuture.allOf(nestedRpc1.errors(i), nestedRpc1.errors(i)));
            when(rpc2.errors(i)).thenAnswer(invocation -> {
                CompletableFuture<Collection<Object>> future = new CompletableFuture<>();

                rpc2responses.add(future);

                return future;
            });

            CompletableFuture<Collection<Object>> future = client.errors(i);

            assertFalse(future.isDone());

            busyWait("RPC2 to get all requests.", () -> rpc2responses.size() == 3);

            rpc2responses.forEach(f -> f.completeExceptionally(TEST_ERROR));

            RpcAggregateException err = expect(RpcAggregateException.class, () -> {
                try {
                    get(future);
                } catch (ExecutionException e) {
                    throw (Exception)e.getCause();
                }
            });

            assertEquals(2, err.errors().size());
            assertEquals(TEST_ERROR.getClass(), err.errors().get(server2.localNode()).getClass());

            RpcAggregateException rpc1Err = (RpcAggregateException)err.errors().get(server1.localNode());

            assertEquals(1, rpc1Err.errors().size());
            assertEquals(TEST_ERROR.getClass(), rpc1Err.errors().get(server2.localNode()).getClass());

            verify(rpc1).errors(i);
            verify(rpc2, times(3)).errors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }
}
