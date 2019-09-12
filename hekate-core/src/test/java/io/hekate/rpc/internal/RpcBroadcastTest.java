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
import io.hekate.rpc.RpcRetry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RpcBroadcastTest extends RpcServiceTestBase {
    @Rpc
    public interface BroadcastRpc {
        @RpcBroadcast
        void call(Object arg);

        @RpcBroadcast(remoteErrors = RpcBroadcast.RemoteErrors.IGNORE)
        void ignoreErrors(Object arg);

        @RpcBroadcast(remoteErrors = RpcBroadcast.RemoteErrors.WARN)
        void warnErrors(Object arg);

        @RpcBroadcast
        void errors(Object arg);

        @RpcRetry
        @RpcBroadcast
        void retry(Object arg);

        @RpcBroadcast
        @RpcRetry(maxAttempts = "1", delay = "10")
        void retryMethod(Object arg);
    }

    private final BroadcastRpc rpc1 = mock(BroadcastRpc.class);

    private final BroadcastRpc rpc2 = mock(BroadcastRpc.class);

    private BroadcastRpc clientRpc;

    private HekateTestNode client;

    private HekateTestNode server1;

    private HekateTestNode server2;

    public RpcBroadcastTest(MultiCodecTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ClientAndServers ctx = prepareClientAndServers(rpc1, rpc2);

        server1 = ctx.servers().get(0);
        server2 = ctx.servers().get(1);

        client = ctx.client();

        clientRpc = client.rpc().clientFor(BroadcastRpc.class)
            .withTimeout(AWAIT_TIMEOUT, TimeUnit.SECONDS)
            .build();
    }

    @Test
    public void testCall() throws Exception {
        repeat(3, i -> {
            clientRpc.call(i);

            verify(rpc1).call(i);
            verify(rpc2).call(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testIgnoreError() throws Exception {
        repeat(3, i -> {
            doThrow(TEST_ERROR).when(rpc1).ignoreErrors(i);
            doThrow(TEST_ERROR).when(rpc2).ignoreErrors(i);

            clientRpc.ignoreErrors(i);

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testIgnorePartialError() throws Exception {
        repeat(3, i -> {
            doThrow(TEST_ERROR).when(rpc2).ignoreErrors(i);

            clientRpc.ignoreErrors(i);

            verify(rpc1).ignoreErrors(i);
            verify(rpc2).ignoreErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testWarnPartialError() throws Exception {
        repeat(3, i -> {
            doThrow(TEST_ERROR).when(rpc2).warnErrors(i);

            clientRpc.warnErrors(i);

            verify(rpc1).warnErrors(i);
            verify(rpc2).warnErrors(i);

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testError() throws Exception {
        repeat(3, i -> {
            doThrow(TEST_ERROR).when(rpc1).errors(i);
            doThrow(TEST_ERROR).when(rpc2).errors(i);

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
            doThrow(TEST_ERROR).when(rpc2).errors(i);

            RpcAggregateException err = expect(RpcAggregateException.class, () -> clientRpc.errors(i));

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
    public void testRecursiveError() throws Exception {
        BroadcastRpc nestedRpc1 = server1.rpc().clientFor(BroadcastRpc.class)
            .withTimeout(1, TimeUnit.SECONDS)
            .forRemotes()
            .build();

        repeat(3, i -> {
            doAnswer(invocationOnMock -> {
                nestedRpc1.errors(i);

                return null;
            }).when(rpc1).errors(i);
            doThrow(TEST_ERROR).when(rpc2).errors(i);

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
        clientRpc = client.rpc().clientFor(BroadcastRpc.class)
            .withRetryPolicy(retry -> retry
                .withFixedDelay(10)
                .maxAttempts(1)
            )
            .build();

        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            doAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return null;
                }
            }).when(rpc1).retry(any());

            clientRpc.retry("test");

            verify(rpc1, times(2)).retry(any());
            verify(rpc2).retry(any());

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testRetryMethodSettings() throws Exception {
        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            doAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return null;
                }
            }).when(rpc1).retryMethod(any());

            clientRpc.retryMethod("test");

            verify(rpc1, times(2)).retryMethod(any());
            verify(rpc2).retryMethod(any());

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }
}
