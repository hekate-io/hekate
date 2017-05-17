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

package io.hekate;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.health.DefaultFailureDetectorConfig;
import io.hekate.cluster.seed.SeedNodeProviderMock;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.metrics.cluster.ClusterMetricsServiceFactory;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.task.TaskServiceFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;

public class HekateNodeTestBase extends HekateTestBase {
    public interface NodeConfigurer {
        void configure(HekateTestNode.Bootstrap b);
    }

    protected SeedNodeProviderMock seedNodes;

    private List<HekateTestNode> allNodes;

    private boolean ignoreNodeFailures;

    @Before
    public void setUp() throws Exception {
        seedNodes = new SeedNodeProviderMock();

        allNodes = new CopyOnWriteArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (!ignoreNodeFailures) {
                allNodes.forEach(HekateTestNode::assertNoNodeFailures);
            }
        } finally {
            try {
                for (HekateTestNode node : allNodes) {
                    try {
                        node.leaveAsync().get(5, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        say("Failed to await for node termination: " + e);

                        System.out.println(threadDump());
                    }
                }
            } finally {
                allNodes.clear();
            }
        }
    }

    protected HekateTestContext context() {
        return HekateTestContext.defaultContext();
    }

    protected void disableNodeFailurePostCheck() {
        ignoreNodeFailures = true;
    }

    protected HekateTestNode createNode() throws Exception {
        return createNode(null);
    }

    protected HekateTestNode createNode(NodeConfigurer configurer) throws Exception {
        HekateTestContext ctx = context();

        InetSocketAddress address = newSocketAddress();

        HekateTestNode.Bootstrap bootstrap = new HekateTestNode.Bootstrap(address);

        bootstrap.setClusterName("test");
        bootstrap.setNodeName("node-" + address.getPort() + '-' + allNodes.size());

        if (ctx.resources() != null) {
            bootstrap.withService(ctx::resources);
        }

        bootstrap.withService(ClusterServiceFactory.class, cluster -> {
            DefaultFailureDetectorConfig fdCfg = new DefaultFailureDetectorConfig();

            fdCfg.setHeartbeatInterval(ctx.hbInterval());
            fdCfg.setHeartbeatLossThreshold(ctx.hbLossThreshold());

            cluster.setGossipInterval(ctx.hbInterval());
            cluster.setSpeedUpGossipSize(10);
            cluster.setSeedNodeProvider(seedNodes);
            cluster.setSplitBrainAction(SplitBrainAction.REJOIN);
            cluster.setFailureDetector(new DefaultFailureDetector(fdCfg));
        });

        bootstrap.withService(NetworkServiceFactory.class, net -> {
            net.setHost(address.getAddress().getHostAddress());
            net.setPort(address.getPort());
            net.setConnectTimeout(ctx.connectTimeout());
            net.setHeartbeatInterval(ctx.hbInterval());
            net.setHeartbeatLossThreshold(ctx.hbLossThreshold());
            net.setAcceptRetryInterval(0);
            net.setNioThreads(ctx.hbLossThreshold());
            net.setTransport(ctx.transport());

            ctx.ssl().ifPresent(net::setSsl);
        });

        bootstrap.withService(TaskServiceFactory.class, tasks ->
            tasks.setLocalExecutionEnabled(false)
        );

        bootstrap.withService(ClusterMetricsServiceFactory.class, metrics ->
            metrics.setEnabled(false)
        );

        if (configurer != null) {
            configurer.configure(bootstrap);
        }

        HekateTestNode node = bootstrap.create();

        allNodes.add(node);

        return node;
    }

    protected void awaitForTopology(HekateTestNode... nodes) {
        awaitForTopology(Arrays.asList(nodes));
    }

    protected void awaitForTopology(List<HekateTestNode> nodes) {
        nodes.forEach(n -> n.awaitForTopology(nodes));
    }

    protected void awaitForTopology(List<HekateTestNode> nodes, HekateTestNode oneMore) {
        List<HekateTestNode> allNodes = new ArrayList<>(nodes);

        allNodes.add(oneMore);

        allNodes.forEach(n -> n.awaitForTopology(allNodes));
    }
}
