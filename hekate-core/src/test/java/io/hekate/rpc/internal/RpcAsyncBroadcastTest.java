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
import io.hekate.rpc.RpcAggregateException;
import io.hekate.rpc.RpcBroadcast;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static java.util.Collections.synchronizedList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcAsyncBroadcastTest extends RpcServiceTestBase {
    @Rpc
    public interface AggregateRpc {
        @RpcBroadcast
        CompletableFuture<Void> call(Object arg);

        @RpcBroadcast(remoteErrors = RpcBroadcast.RemoteErrors.IGNORE)
        CompletableFuture<?> ignoreErrors(Object arg);

        @RpcBroadcast
        CompletableFuture<?> errors(Object arg);
    }

    private final AggregateRpc rpc1 = mock(AggregateRpc.class);

    private final AggregateRpc rpc2 = mock(AggregateRpc.class);

    private AggregateRpc client;

    private HekateTestNode server1;

    private HekateTestNode server2;

    public RpcAsyncBroadcastTest(MultiCodecTestContext ctx) {
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
    public void testCall() throws Exception {
        repeat(3, i -> {
            CompletableFuture<Void> f1 = new CompletableFuture<>();
            CompletableFuture<Void> f2 = new CompletableFuture<>();

            when(rpc1.call(i)).thenReturn(f1);
            when(rpc2.call(i)).thenReturn(f2);

            CompletableFuture<Void> future = client.call(i);

            assertFalse(future.isDone());

            f1.complete(null);
            f2.complete(null);

            assertNull(get(future));

            verify(rpc1).call(i);
            verify(rpc2).call(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncIgnoreError() throws Exception {
        repeat(3, i -> {
            CompletableFuture<?> f1 = new CompletableFuture<>();
            CompletableFuture<?> f2 = new CompletableFuture<>();

            doAnswer(invocationOnMock -> f1).when(rpc1).ignoreErrors(i);
            doAnswer(invocationOnMock -> f2).when(rpc2).ignoreErrors(i);

            CompletableFuture<?> future = client.ignoreErrors(i);

            assertFalse(future.isDone());

            f1.completeExceptionally(TEST_ERROR);
            f2.completeExceptionally(TEST_ERROR);

            assertNull(get(future));

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncIgnorePartialError() throws Exception {
        repeat(3, i -> {
            CompletableFuture<?> f1 = new CompletableFuture<>();
            CompletableFuture<?> f2 = new CompletableFuture<>();

            doAnswer(invocationOnMock -> f1).when(rpc1).ignoreErrors(i);
            doAnswer(invocationOnMock -> f2).when(rpc2).ignoreErrors(i);

            CompletableFuture<?> future = client.ignoreErrors(i);

            assertFalse(future.isDone());

            f1.complete(null);
            f2.completeExceptionally(TEST_ERROR);

            assertNull(get(future));

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testAsyncPartialError() throws Exception {
        repeat(3, i -> {
            CompletableFuture<?> f1 = new CompletableFuture<>();
            CompletableFuture<?> f2 = new CompletableFuture<>();

            doAnswer(invocationOnMock -> f1).when(rpc1).errors(i);
            doAnswer(invocationOnMock -> f2).when(rpc2).errors(i);

            CompletableFuture<?> future = client.errors(i);

            assertFalse(future.isDone());

            f1.complete(null);
            f2.completeExceptionally(TEST_ERROR);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> {
                try {
                    get(future);
                } catch (ExecutionException e) {
                    throw (Exception)e.getCause();
                }
            });

            assertTrue(err.partialResults().isEmpty());

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
            CompletableFuture<?> f1 = new CompletableFuture<>();
            CompletableFuture<?> f2 = new CompletableFuture<>();

            doAnswer(invocationOnMock -> f1).when(rpc1).errors(i);
            doAnswer(invocationOnMock -> f2).when(rpc2).errors(i);

            CompletableFuture<?> future = client.errors(i);

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
    public void testAsyncRecursiveCall() throws Exception {
        AggregateRpc nestedRpc1 = server1.rpc().clientFor(AggregateRpc.class)
            .withTimeout(1, TimeUnit.SECONDS)
            .forRemotes()
            .build();

        repeat(3, i -> {
            List<CompletableFuture<?>> rpc2responses = synchronizedList(new ArrayList<>());

            when(rpc1.call(i)).thenAnswer(invocation -> nestedRpc1.call(i));
            when(rpc2.call(i)).thenAnswer(invocation -> {
                CompletableFuture<Collection<Object>> future = new CompletableFuture<>();

                rpc2responses.add(future);

                return future;
            });

            CompletableFuture<Void> future = client.call(i);

            assertFalse(future.isDone());

            busyWait("RPC2 to get all requests.", () -> rpc2responses.size() == 2);

            rpc2responses.forEach(f -> f.complete(null));

            assertNull(get(future));

            verify(rpc1).call(i);
            verify(rpc2, times(2)).call(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }
}
