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

package io.hekate.messaging;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.codec.CodecFactory;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.partition.RendezvousHashMapper;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MessagingChannelConfigTest extends HekateTestBase {
    private final MessagingChannelConfig<Object> cfg = MessagingChannelConfig.unchecked();

    @Test
    public void testName() {
        assertNull(cfg.getName());

        cfg.setName("test1");

        assertEquals("test1", cfg.getName());

        assertSame(cfg, cfg.withName("test2"));

        assertEquals("test2", cfg.getName());
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
    public void testPartitions() {
        assertEquals(RendezvousHashMapper.DEFAULT_PARTITIONS, cfg.getPartitions());

        cfg.setPartitions(10001);

        assertEquals(10001, cfg.getPartitions());

        assertSame(cfg, cfg.withPartitions(10002));

        assertEquals(10002, cfg.getPartitions());
    }

    @Test
    public void testBackupNodes() {
        assertEquals(0, cfg.getBackupNodes());

        cfg.setBackupNodes(10001);

        assertEquals(10001, cfg.getBackupNodes());

        assertSame(cfg, cfg.withBackupNodes(10002));

        assertEquals(10002, cfg.getBackupNodes());
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
    public void testRetryPolicy() {
        assertNull(cfg.getRetryPolicy());

        GenericRetryConfigurer retry = mock(GenericRetryConfigurer.class);

        cfg.setRetryPolicy(retry);

        assertSame(retry, cfg.getRetryPolicy());

        cfg.setRetryPolicy(null);

        assertNull(cfg.getRetryPolicy());

        assertSame(cfg, cfg.withRetryPolicy(retry));

        assertSame(retry, cfg.getRetryPolicy());
    }

    @Test
    public void testReceiver() {
        MessageReceiver<Object> receiver = msg -> {
            // No-op.
        };

        assertNull(cfg.getReceiver());

        cfg.setReceiver(receiver);

        assertSame(receiver, cfg.getReceiver());

        cfg.setReceiver(null);

        assertNull(cfg.getReceiver());

        assertSame(cfg, cfg.withReceiver(receiver));

        assertSame(receiver, cfg.getReceiver());
    }

    @Test
    public void testHasReceiver() {
        assertFalse(cfg.hasReceiver());

        cfg.setReceiver(msg -> {
            // No-op.
        });

        assertTrue(cfg.hasReceiver());

        cfg.setReceiver(null);

        assertFalse(cfg.hasReceiver());
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
    public void testInterceptors() {
        MessageInterceptor interceptor = mock(MessageInterceptor.class);

        assertNull(cfg.getInterceptors());

        cfg.setInterceptors(Collections.singletonList(interceptor));

        assertEquals(1, cfg.getInterceptors().size());

        cfg.setInterceptors(null);

        assertNull(cfg.getInterceptors());

        assertSame(cfg, cfg.withInterceptor(interceptor));

        assertSame(interceptor, cfg.getInterceptors().get(0));
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
    public void testWarnOnRetry() {
        assertEquals(-1, cfg.getWarnOnRetry());

        cfg.setWarnOnRetry(0);

        assertEquals(0, cfg.getWarnOnRetry());

        assertSame(cfg, cfg.withWarnOnRetry(10));

        assertEquals(10, cfg.getWarnOnRetry());
    }

    @Test
    public void testToString() {
        assertTrue(cfg.toString(), cfg.toString().startsWith(MessagingChannelConfig.class.getSimpleName()));
    }
}
