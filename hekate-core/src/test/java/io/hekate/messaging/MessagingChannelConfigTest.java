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

package io.hekate.messaging;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.codec.CodecFactory;
import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.unicast.LoadBalancer;
import org.junit.Test;
import org.omg.CORBA.Object;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MessagingChannelConfigTest extends HekateTestBase {
    private final MessagingChannelConfig<Object> cfg = new MessagingChannelConfig<>();

    @Test
    public void testName() {
        assertNull(cfg.getName());

        cfg.setName("test1");

        assertEquals("test1", cfg.getName());

        assertSame(cfg, cfg.withName("test2"));

        assertEquals("test2", cfg.getName());

        assertEquals("test3", new MessagingChannelConfig<Object>("test3").getName());
    }

    @Test
    public void testWorkerThreads() {
        assertEquals(0, cfg.getWorkerThreads());

        cfg.setWorkerThreads(10001);

        assertEquals(10001, cfg.getWorkerThreads());

        assertSame(cfg, cfg.withWorkerThreads(10002));

        assertEquals(10002, cfg.getWorkerThreads());
    }

    @Test
    public void testNioThreads() {
        assertEquals(0, cfg.getNioThreads());

        cfg.setNioThreads(10001);

        assertEquals(10001, cfg.getNioThreads());

        assertSame(cfg, cfg.withNioThreads(10002));

        assertEquals(10002, cfg.getNioThreads());
    }

    @Test
    public void testIdleTimeout() {
        assertEquals(0, cfg.getIdleTimeout());

        cfg.setIdleTimeout(10001);

        assertEquals(10001, cfg.getIdleTimeout());

        assertSame(cfg, cfg.withIdleTimeout(10002));

        assertEquals(10002, cfg.getIdleTimeout());
    }

    @Test
    public void testMessagingTimeout() {
        assertEquals(0, cfg.getMessagingTimeout());

        cfg.setMessagingTimeout(10001);

        assertEquals(10001, cfg.getMessagingTimeout());

        assertSame(cfg, cfg.withMessagingTimeout(10002));

        assertEquals(10002, cfg.getMessagingTimeout());
    }

    @Test
    public void testMessageCodec() {
        @SuppressWarnings("unchecked")
        CodecFactory<Object> factory = mock(CodecFactory.class);

        assertNull(cfg.getMessageCodec());

        cfg.setMessageCodec(factory);

        assertSame(factory, cfg.getMessageCodec());

        cfg.setMessageCodec(null);

        assertNull(cfg.getMessageCodec());

        assertSame(cfg, cfg.withMessageCodec(factory));

        assertSame(factory, cfg.getMessageCodec());
    }

    @Test
    public void testClusterFilter() {
        ClusterNodeFilter filter = mock(ClusterNodeFilter.class);

        assertNull(cfg.getClusterFilter());

        cfg.setClusterFilter(filter);

        assertSame(filter, cfg.getClusterFilter());

        cfg.setClusterFilter(null);

        assertNull(cfg.getClusterFilter());

        assertSame(cfg, cfg.withClusterFilter(filter));

        assertSame(filter, cfg.getClusterFilter());
    }

    @Test
    public void testReceiver() {
        @SuppressWarnings("unchecked")
        MessageReceiver<Object> receiver = mock(MessageReceiver.class);

        assertNull(cfg.getReceiver());

        cfg.setReceiver(receiver);

        assertSame(receiver, cfg.getReceiver());

        cfg.setReceiver(null);

        assertNull(cfg.getReceiver());

        assertSame(cfg, cfg.withReceiver(receiver));

        assertSame(receiver, cfg.getReceiver());
    }

    @Test
    public void testFailoverPolicy() {
        FailoverPolicy policy = mock(FailoverPolicy.class);

        assertNull(cfg.getFailoverPolicy());

        cfg.setFailoverPolicy(policy);

        assertSame(policy, cfg.getFailoverPolicy());

        cfg.setFailoverPolicy(null);

        assertNull(cfg.getFailoverPolicy());

        assertSame(cfg, cfg.withFailoverPolicy(policy));

        assertSame(policy, cfg.getFailoverPolicy());
    }

    @Test
    public void testLoadBalancer() {
        @SuppressWarnings("unchecked")
        LoadBalancer<Object> balancer = mock(LoadBalancer.class);

        assertNull(cfg.getLoadBalancer());

        cfg.setLoadBalancer(balancer);

        assertSame(balancer, cfg.getLoadBalancer());

        cfg.setLoadBalancer(null);

        assertNull(cfg.getLoadBalancer());

        assertSame(cfg, cfg.withLoadBalancer(balancer));

        assertSame(balancer, cfg.getLoadBalancer());
    }

    @Test
    public void testLoggerName() {
        assertNull(cfg.getLogCategory());

        cfg.setLogCategory("test1");

        assertEquals("test1", cfg.getLogCategory());

        assertSame(cfg, cfg.withLogCategory("test2"));

        assertEquals("test2", cfg.getLogCategory());
    }

    @Test
    public void testBackPressure() {
        MessagingBackPressureConfig c1 = new MessagingBackPressureConfig();
        MessagingBackPressureConfig c2 = new MessagingBackPressureConfig();

        assertNotNull(cfg.getBackPressure());

        cfg.setBackPressure(c1);

        assertSame(c1, cfg.getBackPressure());

        assertSame(cfg, cfg.withBackPressure(c2));

        assertSame(c2, cfg.getBackPressure());

        expect(IllegalArgumentException.class, () -> cfg.setBackPressure(null));

        assertSame(cfg, cfg.withBackPressure(bp ->
            assertSame(c2, bp)
        ));
    }

    @Test
    public void testToString() {
        assertTrue(cfg.toString(), cfg.toString().startsWith(MessagingChannelConfig.class.getSimpleName()));
    }
}
