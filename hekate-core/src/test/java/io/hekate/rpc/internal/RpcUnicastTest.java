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

import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.UpdatableClusterView;
import io.hekate.codec.CodecException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.partition.PartitionMapper;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcAffinityKey;
import io.hekate.rpc.RpcClientBuilder;
import io.hekate.rpc.RpcClientConfig;
import io.hekate.rpc.RpcException;
import io.hekate.rpc.RpcRetry;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.test.HekateTestError;
import io.hekate.test.NonSerializable;
import io.hekate.test.NonSerializableTestException;
import io.hekate.test.SerializableTestException;
import java.io.InvalidObjectException;
import java.io.NotSerializableException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcUnicastTest extends RpcServiceTestBase {
    @Rpc
    public interface TestRpcA {
        void callA();
    }

    @Rpc
    public interface TestRpcB extends TestRpcA {
        Object callB();
    }

    @Rpc
    public interface TestRpcC extends TestRpcA {
        Object callC(Object arg1, Object arg2);
    }

    @Rpc
    public interface TestRpcD extends TestRpcB, TestRpcC {
        Object callD();
    }

    @Rpc(version = 3)
    public interface TestAffinityRpc extends TestRpcB, TestRpcC {
        Object call(@RpcAffinityKey Object arg);
    }

    @Rpc
    public interface TestRpcWithDefaultMethod {
        Object call();

        default Object callDefault() {
            return call();
        }
    }

    @Rpc
    public interface TestRpcRetry {
        @RpcRetry
        Object retry();

        @RpcRetry(errors = AssertionError.class, maxAttempts = "1", delay = "10")
        Object retryMethod();
    }

    @Rpc
    public interface TestAsyncRpc {
        CompletableFuture<Object> call();
    }

    @Rpc
    public interface TestRpcWithError {
        Object callWithError() throws SerializableTestException;
    }

    public RpcUnicastTest(MultiCodecTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testNonRpcMethod() throws Exception {
        TestRpcB rpc = mock(TestRpcB.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcB proxy = client.rpc().clientFor(TestRpcB.class).build();

        assertNotNull(proxy.toString());
    }

    @Test
    public void testVoid() throws Exception {
        TestRpcA rpc = mock(TestRpcA.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcA proxy = client.rpc().clientFor(TestRpcA.class).build();

        repeat(3, i -> {
            proxy.callA();

            verify(rpc).callA();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testNoArg() throws Exception {
        TestRpcB rpc = mock(TestRpcB.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcB proxy = client.rpc().clientFor(TestRpcB.class).build();

        repeat(3, i -> {
            when(rpc.callB()).thenReturn("result" + i);

            assertEquals("result" + i, proxy.callB());

            verify(rpc).callB();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testArgs() throws Exception {
        TestRpcC rpc = mock(TestRpcC.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcC proxy = client.rpc().clientFor(TestRpcC.class).build();

        repeat(3, i -> {
            when(rpc.callC(i, -i)).thenReturn("result" + i);

            assertEquals("result" + i, proxy.callC(i, -i));

            verify(rpc).callC(i, -i);
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testAffinityKey() throws Exception {
        TestAffinityRpc rpc1 = mock(TestAffinityRpc.class);
        TestAffinityRpc rpc2 = mock(TestAffinityRpc.class);

        ClientAndServers testCtx = prepareClientAndServers(rpc1, rpc2);

        HekateTestNode server1 = testCtx.servers().get(0);
        HekateTestNode server2 = testCtx.servers().get(1);

        TestAffinityRpc proxy = testCtx.client().rpc().clientFor(TestAffinityRpc.class)
            .withLoadBalancer((call, ctx) -> {
                assertSame(TestAffinityRpc.class, call.rpcInterface());
                assertEquals(3, call.rpcVersion());
                assertEquals("call", call.method().getName());
                assertTrue(call.hasArgs());
                assertEquals(1, call.args().length);

                if (ctx.affinityKey().equals("1")) {
                    return server1.cluster().localNode().id();
                } else if (ctx.affinityKey().equals("2")) {
                    return server2.cluster().localNode().id();
                } else {
                    throw new AssertionError("Unexpected affinity key: " + ctx.affinityKey());
                }
            })
            .build();

        repeat(5, i -> {
            if (i % 2 == 0) {
                proxy.call("1");

                verify(rpc1).call("1");
            } else {
                proxy.call("2");

                verify(rpc2).call("2");
            }

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testTag() throws Exception {
        TestAffinityRpc rpc1 = mock(TestAffinityRpc.class);
        TestAffinityRpc rpc2 = mock(TestAffinityRpc.class);

        HekateTestNode server1 = createNode(c ->
            c.withService(RpcServiceFactory.class, f -> {
                f.withServer(new RpcServerConfig()
                    .withTag("test1")
                    .withHandler(rpc1)
                );
            })
        ).join();

        HekateTestNode server2 = createNode(c ->
            c.withService(RpcServiceFactory.class, f -> {
                f.withServer(new RpcServerConfig()
                    .withTag("test2")
                    .withHandler(rpc2)
                );
            })
        ).join();

        HekateTestNode client = createNode().join();

        awaitForTopology(client, server1, server2);

        repeat(5, i -> {
            if (i % 2 == 0) {
                TestAffinityRpc proxy = client.rpc().clientFor(TestAffinityRpc.class, "test1").build();

                proxy.call("1");

                verify(rpc1).call("1");
            } else {
                TestAffinityRpc proxy = client.rpc().clientFor(TestAffinityRpc.class, "test2").build();

                proxy.call("2");

                verify(rpc2).call("2");
            }

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testPartitions() throws Exception {
        TestAffinityRpc rpc1 = mock(TestAffinityRpc.class);
        TestAffinityRpc rpc2 = mock(TestAffinityRpc.class);

        ClientAndServers testCtx = prepareClientAndServers(rpc1, rpc2);

        RpcClientBuilder<TestAffinityRpc> builder = testCtx.client().rpc().clientFor(TestAffinityRpc.class);

        PartitionMapper oldMapper = builder.partitions();

        builder = builder.withPartitions(oldMapper.partitions() * 2, oldMapper.backupNodes() + 3);

        PartitionMapper newMapper = builder.partitions();

        assertEquals(oldMapper.partitions() * 2, newMapper.partitions());
        assertEquals(oldMapper.backupNodes() + 3, newMapper.backupNodes());

        TestAffinityRpc proxy = builder.build();

        repeat(50, i -> {
            String arg = String.valueOf(i);

            proxy.call(arg);

            if (newMapper.map(arg).primaryNode().equals(testCtx.servers().get(0).localNode())) {
                verify(rpc1).call(arg);
            } else {
                verify(rpc2).call(arg);
            }

            verifyNoMoreInteractions(rpc1, rpc2);
            reset(rpc1, rpc2);
        });
    }

    @Test
    public void testCustomClusterView() throws Exception {
        TestRpcA rpc1 = mock(TestRpcA.class);
        TestRpcA rpc2 = mock(TestRpcA.class);

        ClientAndServers testCtx = prepareClientAndServers(rpc1, rpc2);

        UpdatableClusterView customCluster = UpdatableClusterView.empty();

        RpcClientBuilder<TestRpcA> builder = testCtx.client().rpc().clientFor(TestRpcA.class);

        assertNotSame(builder, builder.withCluster(customCluster));

        TestRpcA client = builder.withCluster(customCluster).build();

        expectCause(EmptyTopologyException.class, client::callA);

        verifyNoMoreInteractions(rpc1, rpc2);

        customCluster.update(ClusterTopology.of(1, singleton(testCtx.servers().get(0).localNode())));

        client.callA();

        verify(rpc1).callA();
        verifyNoMoreInteractions(rpc1, rpc2);

        customCluster.update(ClusterTopology.of(2, singleton(testCtx.servers().get(1).localNode())));

        client.callA();

        verify(rpc2).callA();
        verifyNoMoreInteractions(rpc1, rpc2);
    }

    @Test
    public void testDefaultMethod() throws Exception {
        AtomicReference<Object> resultRef = new AtomicReference<>();

        TestRpcWithDefaultMethod rpc = resultRef::get;

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcWithDefaultMethod proxy = client.rpc().clientFor(TestRpcWithDefaultMethod.class).build();

        repeat(3, i -> {
            resultRef.set("result" + i);

            assertEquals("result" + i, proxy.callDefault());
        });
    }

    @Test
    public void testInheritedMethods() throws Exception {
        TestRpcD rpc = mock(TestRpcD.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcD proxy = client.rpc().clientFor(TestRpcD.class).build();

        repeat(3, i -> {
            when(rpc.callB()).thenReturn("result" + i);
            when(rpc.callC(i, -i)).thenReturn("result" + i);
            when(rpc.callD()).thenReturn("result" + -i);

            proxy.callA();

            assertEquals("result" + i, proxy.callC(i, -i));
            assertEquals("result" + i, proxy.callB());
            assertEquals("result" + -i, proxy.callD());

            verify(rpc).callA();
            verify(rpc).callB();
            verify(rpc).callC(i, -i);
            verify(rpc).callD();
            verifyNoMoreInteractions(rpc);

            reset(rpc);
        });
    }

    @Test
    public void testRetryDefaultSettingsInConfig() throws Exception {
        TestRpcRetry rpcApi = mock(TestRpcRetry.class);

        HekateTestNode server = prepareServer(rpcApi, null);

        HekateTestNode client = createNode(boot -> boot
            .withNodeName("rpc-client")
            .withService(RpcServiceFactory.class, rpc -> {
                rpc.withClient(new RpcClientConfig()
                    .withRpcInterface(TestRpcRetry.class)
                    .withRetryPolicy(retry -> retry
                        .maxAttempts(1)
                        .withFixedDelay(10)
                    )
                );
            })
        ).join();

        awaitForTopology(client, server);

        TestRpcRetry proxy = client.rpc().clientFor(TestRpcRetry.class).build();

        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            when(rpcApi.retry()).thenAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return "ok";
                }
            });

            assertEquals("ok", proxy.retry());

            verify(rpcApi, times(2)).retry();
            verifyNoMoreInteractions(rpcApi);
            reset(rpcApi);
        });
    }

    @Test
    public void testRetryDefaultSettingsInBuilder() throws Exception {
        TestRpcRetry rpc = mock(TestRpcRetry.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcRetry proxy = client.rpc().clientFor(TestRpcRetry.class)
            .withRetryPolicy(retry -> retry
                .maxAttempts(1)
                .withFixedDelay(10)
            )
            .build();

        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            when(rpc.retry()).thenAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return "ok";
                }
            });

            assertEquals("ok", proxy.retry());

            verify(rpc, times(2)).retry();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testRetryMethodSettings() throws Exception {
        TestRpcRetry rpc = mock(TestRpcRetry.class);

        TestRpcRetry proxy = prepareClientAndServer(rpc).client().rpc().clientFor(TestRpcRetry.class).build();

        repeat(3, i -> {
            AtomicInteger attempt = new AtomicInteger();

            when(rpc.retryMethod()).thenAnswer(x -> {
                if (attempt.getAndIncrement() == 0) {
                    throw TEST_ERROR;
                } else {
                    return "ok";
                }
            });

            assertEquals("ok", proxy.retryMethod());

            verify(rpc, times(2)).retryMethod();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testUncheckedException() throws Exception {
        TestRpcWithError rpc = mock(TestRpcWithError.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcWithError proxy = client.rpc().clientFor(TestRpcWithError.class).build();

        repeat(3, i -> {
            when(rpc.callWithError()).thenThrow(new RuntimeException(HekateTestError.MESSAGE));

            expectExactMessage(RuntimeException.class, HekateTestError.MESSAGE, proxy::callWithError);

            verify(rpc).callWithError();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testError() throws Exception {
        TestRpcWithError rpc = mock(TestRpcWithError.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcWithError proxy = client.rpc().clientFor(TestRpcWithError.class).build();

        repeat(3, i -> {
            when(rpc.callWithError()).thenThrow(new NoClassDefFoundError(HekateTestError.MESSAGE));

            expectExactMessage(NoClassDefFoundError.class, HekateTestError.MESSAGE, proxy::callWithError);

            verify(rpc).callWithError();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testCheckedException() throws Exception {
        TestRpcWithError rpc = mock(TestRpcWithError.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcWithError proxy = client.rpc().clientFor(TestRpcWithError.class).build();

        repeat(3, i -> {
            when(rpc.callWithError()).thenThrow(new SerializableTestException());

            expectExactMessage(SerializableTestException.class, HekateTestError.MESSAGE, proxy::callWithError);

            verify(rpc).callWithError();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testNonSerializableException() throws Exception {
        TestRpcWithError rpc = mock(TestRpcWithError.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcWithError proxy = client.rpc().clientFor(TestRpcWithError.class).build();

        repeat(3, i -> {
            NonSerializableTestException testError = new NonSerializableTestException(true);

            when(rpc.callWithError()).thenThrow(testError);

            RpcException err = expect(RpcException.class, proxy::callWithError);

            String stackTrace = ErrorUtils.stackTrace(err);

            assertTrue(stackTrace, ErrorUtils.isCausedBy(MessagingRemoteException.class, err));
            assertTrue(stackTrace, stackTrace.contains(NonSerializableTestException.class.getName() + ": " + HekateTestError.MESSAGE));

            verify(rpc).callWithError();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testNonDeserializableException() throws Exception {
        TestRpcWithError rpc = mock(TestRpcWithError.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcWithError proxy = client.rpc().clientFor(TestRpcWithError.class).build();

        repeat(3, i -> {
            NonSerializableTestException testError = new NonSerializableTestException(false);

            when(rpc.callWithError()).thenThrow(testError);

            RpcException err = expect(RpcException.class, proxy::callWithError);

            assertTrue(ErrorUtils.isCausedBy(InvalidObjectException.class, err));
            assertTrue(ErrorUtils.stackTrace(err).contains(InvalidObjectException.class.getName() + ": " + HekateTestError.MESSAGE));

            verify(rpc).callWithError();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testNonSerializableArg() throws Exception {
        TestRpcC rpc = mock(TestRpcC.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcC proxy = client.rpc().clientFor(TestRpcC.class).build();

        repeat(3, i -> {
            RpcException err = expect(RpcException.class, () -> proxy.callC(1, new NonSerializable()));

            String stackTrace = ErrorUtils.stackTrace(err);

            assertTrue(stackTrace, ErrorUtils.isCausedBy(CodecException.class, err));
            assertTrue(stackTrace, stackTrace.contains(NotSerializableException.class.getName()));
            assertTrue(stackTrace, stackTrace.contains(HekateTestError.MESSAGE));

            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testNonSerializableResult() throws Exception {
        TestRpcB rpc = mock(TestRpcB.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestRpcB proxy = client.rpc().clientFor(TestRpcB.class).build();

        repeat(3, i -> {
            when(rpc.callB()).thenReturn(new NonSerializable());

            RpcException err = expect(RpcException.class, proxy::callB);

            String stackTrace = ErrorUtils.stackTrace(err);

            assertTrue(stackTrace, ErrorUtils.isCausedBy(MessagingRemoteException.class, err));
            assertTrue(stackTrace, stackTrace.contains(NotSerializableException.class.getName()));
            assertTrue(stackTrace, stackTrace.contains(HekateTestError.MESSAGE));

            verify(rpc).callB();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testAsyncNonNullResult() throws Exception {
        TestAsyncRpc rpc = mock(TestAsyncRpc.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestAsyncRpc proxy = client.rpc().clientFor(TestAsyncRpc.class).build();

        repeat(3, i -> {
            CompletableFuture<Object> resultFuture = new CompletableFuture<>();

            when(rpc.call()).thenReturn(resultFuture);

            CompletableFuture<Object> callFuture = proxy.call();

            assertFalse(callFuture.isDone());

            resultFuture.complete("OK" + i);

            assertEquals("OK" + i, get(callFuture));

            verify(rpc).call();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testAsyncNullResult() throws Exception {
        TestAsyncRpc rpc = mock(TestAsyncRpc.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestAsyncRpc proxy = client.rpc().clientFor(TestAsyncRpc.class).build();

        repeat(3, i -> {
            CompletableFuture<Object> resultFuture = new CompletableFuture<>();

            when(rpc.call()).thenReturn(resultFuture);

            CompletableFuture<Object> callFuture = proxy.call();

            assertFalse(callFuture.isDone());

            resultFuture.complete(null);

            assertNull(get(callFuture));

            verify(rpc).call();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testAsyncException() throws Exception {
        TestAsyncRpc rpc = mock(TestAsyncRpc.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestAsyncRpc proxy = client.rpc().clientFor(TestAsyncRpc.class).build();

        repeat(3, i -> {
            CompletableFuture<Object> resultFuture = new CompletableFuture<>();

            when(rpc.call()).thenReturn(resultFuture);

            CompletableFuture<Object> callFuture = proxy.call();

            assertFalse(callFuture.isDone());

            resultFuture.completeExceptionally(TEST_ERROR);

            ExecutionException err = expect(ExecutionException.class, () -> get(callFuture));

            assertSame(ErrorUtils.stackTrace(err), TEST_ERROR.getClass(), err.getCause().getClass());

            verify(rpc).call();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testAsyncResultAfterClientLeave() throws Exception {
        CountDownLatch callLatch = new CountDownLatch(1);
        CountDownLatch leaveLatch = new CountDownLatch(1);

        TestAsyncRpc rpc = mock(TestAsyncRpc.class);

        HekateTestNode client = prepareClientAndServer(rpc).client();

        TestAsyncRpc proxy = client.rpc().clientFor(TestAsyncRpc.class).build();

        when(rpc.call()).then(invocation -> {
            callLatch.countDown();

            await(leaveLatch);

            return null;
        });

        // Send RPC request.
        CompletableFuture<Object> callFuture = proxy.call();

        assertFalse(callFuture.isDone());

        // Await for the RPC request to reach the server.
        await(callLatch);

        // Stop the client node.
        client.leave();

        leaveLatch.countDown();

        ExecutionException err = expect(ExecutionException.class, () -> get(callFuture));

        assertSame(MessagingException.class, err.getCause().getClass());

        verify(rpc).call();
        verifyNoMoreInteractions(rpc);
        reset(rpc);
    }

    @Test
    public void testAsyncResultAfterServerLeave() throws Exception {
        TestAsyncRpc rpc = mock(TestAsyncRpc.class);

        ClientAndServer ctx = prepareClientAndServer(rpc);

        TestAsyncRpc proxy = ctx.client().rpc().clientFor(TestAsyncRpc.class).build();

        CountDownLatch serverCall = new CountDownLatch(1);

        CompletableFuture<Object> resultFuture = new CompletableFuture<>();

        when(rpc.call()).then(call -> {
            // Notify on server RPC request.
            serverCall.countDown();

            return resultFuture;
        });

        // Send RPC request.
        CompletableFuture<Object> callFuture = proxy.call();

        // Await for server to receive RPC request.
        await(serverCall);

        assertFalse(callFuture.isDone());

        // Stop the server node.
        ctx.server().leave();

        awaitForTopology(ctx.client());

        ExecutionException err = expect(ExecutionException.class, () -> get(callFuture));

        assertSame(MessagingException.class, err.getCause().getClass());

        verify(rpc).call();
        verifyNoMoreInteractions(rpc);
        reset(rpc);
    }
}
