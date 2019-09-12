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

import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcAggregate;
import io.hekate.rpc.RpcSplit;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.or;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcAsyncSplitAggregateTest extends RpcServiceTestBase {
    @Rpc
    public interface AggregateRpc {
        @RpcAggregate
        CompletableFuture<List<Object>> list(@RpcSplit List<Object> arg);

        @RpcAggregate
        CompletableFuture<Set<Object>> set(@RpcSplit Set<Object> arg);

        @RpcAggregate
        CompletableFuture<Map<Object, Object>> map(@RpcSplit Map<Object, Object> arg);

        @RpcAggregate
        CompletableFuture<Collection<Object>> collection(@RpcSplit Collection<Object> arg);
    }

    private final AggregateRpc rpc1 = mock(AggregateRpc.class);

    private final AggregateRpc rpc2 = mock(AggregateRpc.class);

    private AggregateRpc client;

    public RpcAsyncSplitAggregateTest(MultiCodecTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ClientAndServers ctx = prepareClientAndServers(rpc1, rpc2);

        client = ctx.client().rpc().clientFor(AggregateRpc.class)
            .withTimeout(AWAIT_TIMEOUT, TimeUnit.SECONDS)
            .build();
    }

    @Test
    public void testList() throws Exception {
        repeat(3, i -> {
            CompletableFuture<List<Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<List<Object>> fut2 = new CompletableFuture<>();

            when(rpc1.list(singletonList(i))).thenReturn(fut1);
            when(rpc2.list(singletonList(i))).thenReturn(fut2);

            CompletableFuture<List<Object>> resultFut = client.list(asList(i, i));

            assertFalse(resultFut.isDone());

            fut1.complete(asList(i, i, i));
            fut2.complete(asList(i, i, i));

            List<Object> result = get(resultFut);

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(singletonList(i));
            verify(rpc2).list(singletonList(i));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testLargerList() throws Exception {
        repeat(3, i -> {
            CompletableFuture<List<Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<List<Object>> fut2 = new CompletableFuture<>();

            when(rpc1.list(or(eq(asList(i + 1, i + 3)), eq(asList(i + 2, i + 4))))).thenReturn(fut1);
            when(rpc2.list(or(eq(asList(i + 1, i + 3)), eq(asList(i + 2, i + 4))))).thenReturn(fut2);

            CompletableFuture<List<Object>> resultFut = client.list(asList(i + 1, i + 2, i + 3, i + 4));

            assertFalse(resultFut.isDone());

            fut1.complete(asList(i, i, i));
            fut2.complete(asList(i, i, i));

            List<Object> result = get(resultFut);

            assertEquals(6, result.size());
            assertTrue(result.stream().allMatch(r -> (Integer)r == i));

            verify(rpc1).list(or(eq(asList(i + 1, i + 3)), eq(asList(i + 2, i + 4))));
            verify(rpc2).list(or(eq(asList(i + 1, i + 3)), eq(asList(i + 2, i + 4))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testListPartialNull() throws Exception {
        repeat(3, i -> {
            CompletableFuture<List<Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<List<Object>> fut2 = new CompletableFuture<>();

            when(rpc1.list(singletonList(i))).thenReturn(fut1);
            when(rpc2.list(singletonList(i))).thenReturn(fut2);

            CompletableFuture<List<Object>> resultFut = client.list(asList(i, i));

            assertFalse(resultFut.isDone());

            fut1.complete(asList(i, i, i));
            fut2.complete(null);

            List<Object> result = get(resultFut);

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
            CompletableFuture<Set<Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<Set<Object>> fut2 = new CompletableFuture<>();

            when(rpc1.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(fut1);
            when(rpc2.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(fut2);

            CompletableFuture<Set<Object>> resultFut = client.set(toSet(i + 1, i + 2));

            assertFalse(resultFut.isDone());

            fut1.complete(toSet("a" + i, "b" + i, "c" + i));
            fut2.complete(toSet("d" + i, "e" + i, "f" + i));

            Set<Object> result = get(resultFut);

            assertEquals(result, toSet("a" + i, "b" + i, "c" + i, "d" + i, "e" + i, "f" + i));

            verify(rpc1).set(or(eq(singleton(i + 1)), eq(singleton(i + 2))));
            verify(rpc2).set(or(eq(singleton(i + 1)), eq(singleton(i + 2))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testLargerSet() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Set<Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<Set<Object>> fut2 = new CompletableFuture<>();

            when(rpc1.set(or(eq(toSet(i + 1, i + 3)), eq(toSet(i + 2, i + 4))))).thenReturn(fut1);
            when(rpc2.set(or(eq(toSet(i + 1, i + 3)), eq(toSet(i + 2, i + 4))))).thenReturn(fut2);

            CompletableFuture<Set<Object>> resultFut = client.set(toSet(i + 1, i + 2, i + 3, i + 4));

            assertFalse(resultFut.isDone());

            fut1.complete(toSet("a" + i));
            fut2.complete(toSet("b" + i));

            Set<Object> result = get(resultFut);

            assertEquals(result, toSet("a" + i, "b" + i));

            verify(rpc1).set(or(eq(toSet(i + 1, i + 3)), eq(toSet(i + 2, i + 4))));
            verify(rpc2).set(or(eq(toSet(i + 1, i + 3)), eq(toSet(i + 2, i + 4))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testSetPartialNull() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Set<Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<Set<Object>> fut2 = new CompletableFuture<>();

            when(rpc1.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(fut1);
            when(rpc2.set(or(eq(singleton(i + 1)), eq(singleton(i + 2))))).thenReturn(fut2);

            CompletableFuture<Set<Object>> resultFut = client.set(toSet(i + 1, i + 2));

            assertFalse(resultFut.isDone());

            fut1.complete(toSet("a" + i, "b" + i, "c" + i));
            fut2.complete(null);

            Set<Object> result = get(resultFut);

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
            CompletableFuture<Map<Object, Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<Map<Object, Object>> fut2 = new CompletableFuture<>();

            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));
            Map<Object, Object> m2 = Stream.of("d" + i, "e" + i, "f" + i).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(fut1);
            when(rpc2.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(fut2);

            Map<Object, Object> args = new HashMap<>();
            args.put(i + 1, i);
            args.put(i + 2, i);

            CompletableFuture<Map<Object, Object>> resultFut = client.map(args);

            assertFalse(resultFut.isDone());

            fut1.complete(m1);
            fut2.complete(m2);

            Map<Object, Object> result = get(resultFut);

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
    public void testLargerMap() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Map<Object, Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<Map<Object, Object>> fut2 = new CompletableFuture<>();

            Map<Object, Object> m1 = Stream.of(i + 1, i + 3).collect(toMap(o -> o, o -> o + "test"));
            Map<Object, Object> m2 = Stream.of(i + 2, i + 4).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(or(eq(m1), eq(m2)))).thenReturn(fut1);
            when(rpc2.map(or(eq(m1), eq(m2)))).thenReturn(fut2);

            Map<Object, Object> args = new HashMap<>();
            args.putAll(m1);
            args.putAll(m2);

            CompletableFuture<Map<Object, Object>> resultFut = client.map(args);

            assertFalse(resultFut.isDone());

            fut1.complete(m1);
            fut2.complete(m2);

            Map<Object, Object> result = get(resultFut);

            Map<Object, Object> expected = new HashMap<>();
            expected.putAll(m1);
            expected.putAll(m2);

            assertEquals(expected, result);

            verify(rpc1).map(or(eq(m1), eq(m2)));
            verify(rpc2).map(or(eq(m1), eq(m2)));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testMapPartialNull() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Map<Object, Object>> fut1 = new CompletableFuture<>();
            CompletableFuture<Map<Object, Object>> fut2 = new CompletableFuture<>();

            Map<Object, Object> m1 = Stream.of("a" + i, "b" + i, "c" + i).collect(toMap(o -> o, o -> o + "test"));

            when(rpc1.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(fut1);
            when(rpc2.map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))))).thenReturn(fut2);

            Map<Object, Object> args = new HashMap<>();
            args.put(i + 1, i);
            args.put(i + 2, i);

            CompletableFuture<Map<Object, Object>> resultFut = client.map(args);

            assertFalse(resultFut.isDone());

            fut1.complete(m1);
            fut2.complete(null);

            Map<Object, Object> result = get(resultFut);

            Map<Object, Object> expected = new HashMap<>(m1);

            assertEquals(expected, result);

            verify(rpc1).map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))));
            verify(rpc2).map(or(eq(singletonMap(i + 1, i)), eq(singletonMap(i + 2, i))));

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }
}
