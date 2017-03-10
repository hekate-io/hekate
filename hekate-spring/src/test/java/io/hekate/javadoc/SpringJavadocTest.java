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

package io.hekate.javadoc;

import io.hekate.HekateTestBase;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.Hekate;
import io.hekate.election.ElectionService;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingService;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.cluster.ClusterMetricsService;
import java.util.function.BiConsumer;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SpringJavadocTest extends HekateTestBase {
    @Test
    public void testNodeSimpleBean() {
        doTest("javadoc/simple-bean.xml");
    }

    @Test
    public void testNodeSimpleXsd() {
        doTest("javadoc/simple-xsd.xml");
    }

    @Test
    public void testNodeCompleteXsd() {
        doTest("javadoc/complete-xsd.xml");
    }

    @Test
    public void testNodeCompleteBean() {
        doTest("javadoc/complete-bean.xml");
    }

    @Test
    public void testClusterXsd() {
        doTest("javadoc/cluster/service-xsd.xml");
    }

    @Test
    public void testClusterBean() {
        doTest("javadoc/cluster/service-bean.xml");
    }

    @Test
    public void testMessagingXsd() {
        doTest("javadoc/messaging/service-xsd.xml", (node, ctx) -> {
            assertNotNull(node.get(MessagingService.class).get("example.channel"));

            MessagingChannel<?> channel = ctx.getBean("example.channel", MessagingChannel.class);

            assertNotNull(channel);
        });
    }

    @Test
    public void testMessagingBean() {
        doTest("javadoc/messaging/service-bean.xml", (node, ctx) ->
            assertNotNull(node.get(MessagingService.class).get("example.channel"))
        );
    }

    @Test
    public void testMessagingChannelOptionsXsd() {
        doTest("javadoc/messaging/channel-opts-xsd.xml", (node, ctx) -> {
            assertNotNull(node.get(MessagingService.class).get("example.channel"));

            MessagingChannel<?> channel = ctx.getBean("example.channel", MessagingChannel.class);

            assertNotNull(channel);
        });
    }

    @Test
    public void testMessagingChannelOptionsBean() {
        doTest("javadoc/messaging/channel-opts-bean.xml", (node, ctx) ->
            assertNotNull(node.get(MessagingService.class).get("example.channel"))
        );
    }

    @Test
    public void testTaskXsd() {
        doTest("javadoc/task/service-xsd.xml");
    }

    @Test
    public void testTaskBean() {
        doTest("javadoc/task/service-bean.xml");
    }

    @Test
    public void testLockXsd() {
        doTest("javadoc/lock/service-xsd.xml", (node, ctx) -> {
            assertNotNull(node.get(LockService.class).get("region1"));
            assertNotNull(node.get(LockService.class).get("region2"));

            assertNotNull(ctx.getBean("region1", LockRegion.class));
            assertNotNull(ctx.getBean("region2", LockRegion.class));
        });
    }

    @Test
    public void testLockBean() {
        doTest("javadoc/lock/service-bean.xml", (node, ctx) -> {
            assertNotNull(node.get(LockService.class).get("region1"));
            assertNotNull(node.get(LockService.class).get("region2"));
        });
    }

    @Test
    public void testElectionXsd() {
        doTest("javadoc/election/service-xsd.xml", (node, ctx) ->
            assertEquals(node.getNode(), node.get(ElectionService.class).getLeader("example.election.group").join())
        );
    }

    @Test
    public void testElectionBean() {
        doTest("javadoc/election/service-bean.xml", (node, ctx) ->
            assertEquals(node.getNode(), node.get(ElectionService.class).getLeader("example.election.group").join())
        );
    }

    @Test
    public void testCoordinationXsd() {
        doTest("javadoc/coordinate/service-xsd.xml", (node, ctx) ->
            assertNotNull(node.get(CoordinationService.class).get("example.process").getFuture().join())
        );
    }

    @Test
    public void testCoordinationBean() {
        doTest("javadoc/coordinate/service-bean.xml", (node, ctx) ->
            assertNotNull(node.get(CoordinationService.class).get("example.process").getFuture().join())
        );
    }

    @Test
    public void testLocalMetricsXsd() {
        doTest("javadoc/metrics/service-xsd.xml", (node, ctx) -> {
            assertNotNull(node.get(MetricsService.class).getCounter("example.counter"));
            assertNotNull(node.get(MetricsService.class).getMetric("example.probe"));
        });
    }

    @Test
    public void testMetricsBean() {
        doTest("javadoc/metrics/service-bean.xml", (node, ctx) -> {
            assertNotNull(node.get(MetricsService.class).getCounter("example.counter"));
            assertNotNull(node.get(MetricsService.class).getMetric("example.probe"));
        });
    }

    @Test
    public void testClusterMetricsXsd() {
        doTest("javadoc/metrics/cluster/service-xsd.xml", (node, ctx) ->
            assertTrue(node.get(ClusterMetricsService.class).forNode(node.getNode()).isPresent())
        );
    }

    @Test
    public void testClusterMetricsBean() {
        doTest("javadoc/metrics/cluster/service-bean.xml", (node, ctx) ->
            assertTrue(node.get(ClusterMetricsService.class).forNode(node.getNode()).isPresent())
        );
    }

    @Test
    public void testNetworkXsd() {
        doTest("javadoc/network/service-xsd.xml");
    }

    @Test
    public void testNetworkBean() {
        doTest("javadoc/network/service-bean.xml");
    }

    private void doTest(String resource) {
        doTest(resource, null);
    }

    private void doTest(String resource, BiConsumer<Hekate, ApplicationContext> task) {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(resource);

        Hekate instance = ctx.getBean("hekate", Hekate.class);

        assertSame(Hekate.State.UP, instance.getState());

        if (task != null) {
            task.accept(instance, ctx);
        }

        ctx.close();

        assertSame(Hekate.State.DOWN, instance.getState());
    }
}
