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

import io.hekate.core.HekateConfigurationException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.MessagingChannelClosedException;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcClientBuilder;
import io.hekate.rpc.RpcClientConfig;
import io.hekate.rpc.RpcException;
import io.hekate.rpc.RpcLoadBalancer;
import io.hekate.rpc.RpcRequest;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServerInfo;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.util.format.ToString;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RpcServiceTest extends RpcServiceTestBase {
    @Rpc
    public interface TestRpcA {
        void callA();
    }

    @Rpc
    public interface TestRpcB extends TestRpcA {
        @SuppressWarnings("unused")
        Object callB();
    }

    public RpcServiceTest(MultiCodecTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testServers() throws Exception {
        TestRpcA rpcA = mock(TestRpcA.class);
        TestRpcB rpcB = mock(TestRpcB.class);

        HekateTestNode node = createNode(boot ->
            boot.withService(RpcServiceFactory.class, rpc -> {
                    rpc.withServer(new RpcServerConfig()
                        .withHandler(rpcA)
                    );
                    rpc.withServer(new RpcServerConfig()
                        .withHandler(rpcB)
                        .withTag("A")
                        .withTag("B")
                    );
                }
            )
        ).join();

        RpcService rpc = node.rpc();

        List<RpcServerInfo> servers = rpc.servers();

        assertSame(servers, rpc.servers());
        expect(UnsupportedOperationException.class, () -> servers.add(mock(RpcServerInfo.class)));

        RpcServerInfo serverA = servers.stream().filter(s -> s.rpc() == rpcA).findFirst().orElseThrow(AssertionError::new);
        RpcServerInfo serverB = servers.stream().filter(s -> s.rpc() == rpcB).findFirst().orElseThrow(AssertionError::new);

        assertSame(rpcA, serverA.rpc());
        assertTrue(serverA.tags().isEmpty());
        assertEquals(1, serverA.interfaces().size());
        assertEquals(1, serverA.interfaces().get(0).methods().size());

        assertSame(rpcB, serverB.rpc());
        assertEquals(toSet("A", "B"), serverB.tags());
        assertEquals(2, serverB.interfaces().size());

        assertTrue(serverB.interfaces().stream().anyMatch(t -> t.javaType() == TestRpcA.class));
        assertTrue(serverB.interfaces().stream().anyMatch(t -> t.javaType() == TestRpcB.class));

        assertEquals(ToString.format(serverA), serverA.toString());
        assertEquals(ToString.format(serverB), serverB.toString());
    }

    @Test
    public void failIfRpcServerIsNotAnRpcInterface() throws Exception {
        Object notAnRpc = new Object();

        HekateConfigurationException err = expect(HekateConfigurationException.class, () -> createNode(boot ->
            boot.withService(RpcServiceFactory.class, rpc -> {
                    rpc.withServer(new RpcServerConfig()
                        .withHandler(notAnRpc)
                    );
                }
            )
        ));

        assertEquals(
            RpcServerConfig.class.getSimpleName() + ": RPC handler must implement at least one @Rpc-annotated public interface "
                + "[handler=" + notAnRpc + ']',
            err.getMessage()
        );
    }

    @Test
    public void failIfRpcServerHasInvalidTag() throws Exception {
        HekateConfigurationException err = expect(HekateConfigurationException.class, () -> createNode(boot ->
            boot.withService(RpcServiceFactory.class, rpc -> {
                    rpc.withServer(new RpcServerConfig()
                        .withTag("invalid tag")
                        .withHandler(mock(TestRpcA.class))
                    );
                }
            )
        ));

        assertEquals(
            RpcServerConfig.class.getSimpleName() + ": tag can contain only alpha-numeric characters and non-repeatable "
                + "dots/hyphens [value=invalid tag]",
            err.getMessage()
        );
    }

    @Test
    public void failIfRpcClientHasInvalidTag() throws Exception {
        HekateConfigurationException err = expect(HekateConfigurationException.class, () -> createNode(boot ->
            boot.withService(RpcServiceFactory.class, rpc -> {
                    rpc.withClient(new RpcClientConfig()
                        .withTag("invalid tag")
                        .withRpcInterface(TestRpcA.class)
                    );
                }
            )
        ));

        assertEquals(
            RpcClientConfig.class.getSimpleName() + ": tag can contain only alpha-numeric characters and non-repeatable "
                + "dots/hyphens [value=invalid tag]",
            err.getMessage()
        );
    }

    @Test
    public void testClientForReturnsSameInstance() throws Exception {
        HekateTestNode node = createNode().join();

        RpcService rpc = node.rpc();

        RpcClientBuilder<TestRpcA> a1 = rpc.clientFor(TestRpcA.class);
        RpcClientBuilder<TestRpcA> a2 = rpc.clientFor(TestRpcA.class);

        assertSame(a1, a2);

        RpcClientBuilder<TestRpcA> a3 = rpc.clientFor(TestRpcA.class, "test-tag");
        RpcClientBuilder<TestRpcA> a4 = rpc.clientFor(TestRpcA.class, "test-tag");

        assertSame(a3, a4);

        assertNotSame(a2, a3);

        node.leave().join();

        RpcClientBuilder<TestRpcA> b1 = rpc.clientFor(TestRpcA.class);
        RpcClientBuilder<TestRpcA> b2 = rpc.clientFor(TestRpcA.class);

        assertSame(b1, b2);

        RpcClientBuilder<TestRpcA> b3 = rpc.clientFor(TestRpcA.class, "test-tag");
        RpcClientBuilder<TestRpcA> b4 = rpc.clientFor(TestRpcA.class, "test-tag");

        assertSame(b3, b4);

        assertNotSame(b2, b3);

        assertNotSame(a1, b1);
        assertNotSame(a2, b2);
        assertNotSame(a3, b3);
        assertNotSame(a4, b4);
    }

    @Test
    public void testClientBuilder() throws Exception {
        TestRpcA rpc = mock(TestRpcA.class);

        ClientAndServer ctx = prepareClientAndServer(rpc);

        RpcLoadBalancer lb = mock(RpcLoadBalancer.class);

        RpcClientBuilder<TestRpcA> builder = ctx.client().rpc().clientFor(TestRpcA.class)
            .withLoadBalancer(lb)
            .withTimeout(3, TimeUnit.SECONDS);

        assertSame(TestRpcA.class, builder.type());
        assertEquals(3000, builder.timeout());
        assertTrue(builder.cluster().topology().contains(ctx.server().localNode()));
        assertFalse(builder.cluster().topology().contains(ctx.client().localNode()));

        assertEquals(ToString.format(builder), builder.toString());

        TestRpcA proxy = builder.build();

        repeat(3, i -> {
            when(lb.route(any(RpcRequest.class), any(LoadBalancerContext.class))).thenReturn(ctx.server().localNode().id());

            proxy.callA();

            verify(lb).route(any(RpcRequest.class), any(LoadBalancerContext.class));
            verifyNoMoreInteractions(lb);
            reset(lb);

            verify(rpc).callA();
            verifyNoMoreInteractions(rpc);
            reset(rpc);
        });
    }

    @Test
    public void testClusterOfType() throws Exception {
        TestRpcA rpc = mock(TestRpcA.class);

        ClientAndServer ctx = prepareClientAndServer(rpc);

        assertTrue(ctx.client().rpc().clusterOf(TestRpcA.class).topology().contains(ctx.server().localNode()));
        assertFalse(ctx.client().rpc().clusterOf(TestRpcA.class).topology().contains(ctx.client().localNode()));
    }

    @Test
    public void testClusterOfTypeAndTag() throws Exception {
        TestRpcA rpc = mock(TestRpcA.class);

        ClientAndServer ctx = prepareClientAndServer(rpc, "test");

        assertTrue(ctx.client().rpc().clusterOf(TestRpcA.class, "test").topology().contains(ctx.server().localNode()));
        assertFalse(ctx.client().rpc().clusterOf(TestRpcA.class, "test").topology().contains(ctx.client().localNode()));
    }

    @Test
    public void testRejoin() throws Exception {
        TestRpcA rpc = mock(TestRpcA.class);

        ClientAndServer ctx = prepareClientAndServer(rpc);

        TestRpcA client = ctx.client().rpc().clientFor(TestRpcA.class).build();

        repeat(3, i -> {
            client.callA();

            ctx.client().leave();

            RpcException err = expect(RpcException.class, client::callA);

            assertTrue(err.isCausedBy(MessagingChannelClosedException.class));

            ctx.client().join();

            client.callA();
        });
    }

    @Test
    public void testPreConfiguredClientBuilder() throws Exception {
        HekateTestNode server = createNode(boot ->
            boot.withService(RpcServiceFactory.class)
                .withServer(new RpcServerConfig()
                    .withTag("test-tag")
                    .withHandler(mock(TestRpcA.class))
                )
        ).join();

        RpcLoadBalancer lb = mock(RpcLoadBalancer.class);

        when(lb.route(any(RpcRequest.class), any(LoadBalancerContext.class))).thenReturn(server.cluster().localNode().id());

        HekateTestNode client = createNode(boot ->
            boot.withService(RpcServiceFactory.class, rpc ->
                rpc.withClient(new RpcClientConfig()
                    .withTag("test-tag")
                    .withRpcInterface(TestRpcA.class)
                    .withLoadBalancer(lb)
                    .withTimeout(100500)
                    .withPartitions(RendezvousHashMapper.DEFAULT_PARTITIONS * 2)
                    .withBackupNodes(100500)
                )
            )
        ).join();

        RpcClientBuilder<TestRpcA> builder = client.rpc().clientFor(TestRpcA.class, "test-tag");

        assertSame(TestRpcA.class, builder.type());
        assertEquals("test-tag", builder.tag());
        assertEquals(100500, builder.timeout());
        assertEquals(RendezvousHashMapper.DEFAULT_PARTITIONS * 2, builder.partitions().partitions());
        assertEquals(100500, builder.partitions().backupNodes());

        builder.build().callA();

        verify(lb).route(any(RpcRequest.class), any(LoadBalancerContext.class));
        verifyNoMoreInteractions(lb);
    }

    @Test
    public void testCanRegisterSameServerWithTag() throws Exception {
        // Check same type.
        createNode(c ->
            c.withService(RpcServiceFactory.class, f -> {
                f.withServer(new RpcServerConfig()
                    .withHandler(mock(TestRpcA.class))
                    .withTag("test-1")
                );
                f.withServer(new RpcServerConfig()
                    .withHandler(mock(TestRpcA.class))
                    .withTag("test-2")
                );
            })
        ).join();

        // Check interfaces inheritance.
        createNode(c ->
            c.withService(RpcServiceFactory.class, f -> {
                f.withServer(new RpcServerConfig()
                    .withHandler(mock(TestRpcA.class))
                    .withTag("test-1")
                );
                f.withServer(new RpcServerConfig()
                    .withHandler(mock(TestRpcB.class))
                    .withTag("test-2")
                );
            })
        ).join();
    }

    @Test
    public void testTags() throws Exception {
        TestRpcA rpc1 = mock(TestRpcA.class);
        TestRpcA rpc2 = mock(TestRpcA.class);

        createNode(c ->
            c.withService(RpcServiceFactory.class, f -> {
                f.withServer(new RpcServerConfig()
                    .withHandler(rpc1)
                    .withTag("test-1")
                );
                f.withServer(new RpcServerConfig()
                    .withHandler(rpc2)
                    .withTag("test-2")
                );
            })
        ).join();

        RpcService rpc = createNode().join().rpc();

        TestRpcA client1 = rpc.clientFor(TestRpcA.class, "test-1").build();
        TestRpcA client2 = rpc.clientFor(TestRpcA.class, "test-2").build();

        client1.callA();

        verify(rpc1).callA();

        client2.callA();

        verify(rpc2).callA();
    }

    @Test
    public void testCanNotRegisterSameRpcTwice() throws Exception {
        // Check same type.
        expectExactMessage(
            HekateConfigurationException.class,
            RpcServerConfig.class.getSimpleName() + ": Can't register the same RPC interface multiple times "
                + "[key=RpcTypeKey[type=" + TestRpcA.class.getName() + "]]",
            () -> createNode(c ->
                c.withService(RpcServiceFactory.class, f -> {
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcA.class))
                    );
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcA.class))
                    );
                })
            )
        );

        // Check same type with tag.
        expectExactMessage(
            HekateConfigurationException.class,
            RpcServerConfig.class.getSimpleName() + ": Can't register the same RPC interface multiple times "
                + "[key=RpcTypeKey[type=" + TestRpcA.class.getName() + ", tag=test]]",
            () -> createNode(c ->
                c.withService(RpcServiceFactory.class, f -> {
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcA.class))
                        .withTag("test")
                    );
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcA.class))
                        .withTag("test")
                    );
                })
            )
        );

        // Check interfaces inheritance.
        expectExactMessage(
            HekateConfigurationException.class,
            RpcServerConfig.class.getSimpleName() + ": Can't register the same RPC interface multiple times "
                + "[key=RpcTypeKey[type=" + TestRpcA.class.getName() + "]]",
            () -> createNode(c ->
                c.withService(RpcServiceFactory.class, f -> {
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcA.class))
                    );
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcB.class))
                    );
                })
            )
        );

        // Check interfaces inheritance with tag.
        expectExactMessage(
            HekateConfigurationException.class,
            RpcServerConfig.class.getSimpleName() + ": Can't register the same RPC interface multiple times "
                + "[key=RpcTypeKey[type=" + TestRpcA.class.getName() + ", tag=test]]",
            () -> createNode(c ->
                c.withService(RpcServiceFactory.class, f -> {
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcA.class))
                        .withTag("test")
                    );
                    f.withServer(new RpcServerConfig()
                        .withHandler(mock(TestRpcB.class))
                        .withTag("test")
                    );
                })
            )
        );
    }
}
