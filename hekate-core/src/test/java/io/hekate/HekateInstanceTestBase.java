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
import io.hekate.core.HekateTestInstance;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;

public class HekateInstanceTestBase extends HekateTestBase {
    public interface InstanceConfigurer {
        void configure(HekateTestInstance.Bootstrap b);
    }

    protected SeedNodeProviderMock seedNodes;

    private List<HekateTestInstance> instances;

    private boolean ignoreNodeFailures;

    @Before
    public void setUp() throws Exception {
        seedNodes = new SeedNodeProviderMock();

        instances = new CopyOnWriteArrayList<>();
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (!ignoreNodeFailures) {
                instances.forEach(HekateTestInstance::assertNoNodeFailures);
            }
        } finally {
            try {
                for (HekateTestInstance instance : instances) {
                    try {
                        instance.leaveAsync().get(5, TimeUnit.SECONDS);
                    } catch (TimeoutException e) {
                        say("Failed to await for node termination: " + e);

                        System.out.println(threadDump());
                    }
                }
            } finally {
                instances.clear();
            }
        }
    }

    protected void disableNodeFailurePostCheck() {
        ignoreNodeFailures = true;
    }

    protected void awaitForTopology(HekateTestInstance... nodes) {
        awaitForTopology(Arrays.asList(nodes));
    }

    protected void awaitForTopology(List<HekateTestInstance> nodes) {
        nodes.forEach(n -> n.awaitForTopology(nodes));
    }

    protected void awaitForTopology(List<HekateTestInstance> nodes, HekateTestInstance oneMore) {
        List<HekateTestInstance> allNodes = new ArrayList<>(nodes);

        allNodes.add(oneMore);

        allNodes.forEach(n -> n.awaitForTopology(allNodes));
    }

    protected HekateTestInstance createInstance() throws Exception {
        return createInstance(null);
    }

    protected HekateTestInstance createInstance(InstanceConfigurer configurer) throws Exception {
        InetSocketAddress address = newSocketAddress();

        HekateTestInstance.Bootstrap bootstrap = new HekateTestInstance.Bootstrap(address);

        bootstrap.setClusterName("test");
        bootstrap.setNodeName("node-" + address.getPort() + '-' + instances.size());

        DefaultFailureDetectorConfig fdCfg = new DefaultFailureDetectorConfig();

        fdCfg.setHeartbeatInterval(100);
        fdCfg.setHeartbeatLossThreshold(3);

        ClusterServiceFactory cluster = bootstrap.find(ClusterServiceFactory.class).get();

        cluster.setGossipInterval(100);
        cluster.setSpeedUpGossipSize(10);
        cluster.setSeedNodeProvider(seedNodes);
        cluster.setSplitBrainAction(SplitBrainAction.REJOIN);
        cluster.setFailureDetector(new DefaultFailureDetector(fdCfg));

        NetworkServiceFactory net = new NetworkServiceFactory();

        net.setHost(address.getAddress().getHostAddress());
        net.setPort(address.getPort());
        net.setConnectTimeout(300);
        net.setHeartbeatInterval(100);
        net.setHeartbeatLossThreshold(3);
        net.setAcceptRetryInterval(0);

        bootstrap.withService(net);

        if (configurer != null) {
            configurer.configure(bootstrap);
        }

        HekateTestInstance instance = bootstrap.createInstance();

        instances.add(instance);

        return instance;
    }
}
