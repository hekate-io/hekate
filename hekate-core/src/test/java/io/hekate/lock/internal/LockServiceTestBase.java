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

package io.hekate.lock.internal;

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockServiceFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class LockServiceTestBase extends HekateNodeParamTestBase {
    public interface Configurer {
        void configure(LockServiceFactory factory);
    }

    public static class LockTestContext extends HekateTestContext {
        private final int workerThreads;

        private final int nioThreads;

        public LockTestContext(HekateTestContext src, int workerThreads, int nioThreads) {
            super(src);

            this.workerThreads = workerThreads;
            this.nioThreads = nioThreads;
        }
    }

    public static final String REGION_1 = "region1";

    public static final String REGION_2 = "region2";

    private final LockTestContext ctx;

    public LockServiceTestBase(LockTestContext ctx) {
        super(ctx);

        this.ctx = ctx;
    }

    protected HekateTestNode createLockNode() throws Exception {
        return createLockNode(null);
    }

    protected HekateTestNode createLockNode(Configurer configurer) throws Exception {
        return createNode(c -> {
            LockServiceFactory factory = new LockServiceFactory()
                .withRetryInterval(10)
                .withNioThreads(ctx.nioThreads)
                .withWorkerThreads(ctx.workerThreads)
                .withRegion(new LockRegionConfig().withName(REGION_1))
                .withRegion(new LockRegionConfig().withName(REGION_2));

            if (configurer != null) {
                configurer.configure(factory);
            }

            c.withService(factory);
        });
    }

    protected HekateTestNode awaitForQueuedLock(String lock, HekateTestNode owner, List<HekateTestNode> nodes) throws Exception {
        AtomicReference<HekateTestNode> result = new AtomicReference<>();

        busyWait("queued lock [lock=" + lock + ']', () -> {
            for (HekateTestNode node : nodes) {
                List<ClusterNodeId> locks = node.get(DefaultLockService.class).region(REGION_1).queueOf(lock);

                if (locks.contains(owner.localNode().id())) {
                    result.set(node);

                    return true;
                }
            }

            return false;
        });

        assertNotNull(result.get());

        return result.get();
    }

    protected String getRemotelyManagedLockName(HekateTestNode node) throws Exception {
        return getRemotelyManagedLockName("test", node);
    }

    protected String getRemotelyManagedLockName(String lockPrefix, HekateTestNode node) throws Exception {
        DefaultLockRegion region = node.get(DefaultLockService.class).region(REGION_1);

        for (int i = 0; i < BUSY_WAIT_LOOPS; i++) {
            String lockName = lockPrefix + i;

            if (!region.managerOf(lockName).equals(node.localNode().id())) {
                return lockName;
            }

            Thread.sleep(BUSY_WAIT_INTERVAL);
        }

        throw new AssertionError("Failed to find remotely managed lock.");
    }

    protected HekateTestNode getLockManagerNode(String lock, List<HekateTestNode> nodes) {
        assertTrue(nodes.size() > 1);

        ClusterNodeId nodeId = nodes.get(0).get(DefaultLockService.class).region(REGION_1).managerOf(lock);

        return nodes.stream()
            .filter(n -> n.localNode().id().equals(nodeId))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Failed to find lock manager node [lock=" + lock + ']'));
    }

    protected void awaitForLockTopology(List<HekateTestNode> nodes) throws Exception {
        busyWait("consistent lock topology", () -> {
            Set<ClusterHash> topologies = new HashSet<>();

            nodes.forEach(n -> {
                ClusterHash topology = n.get(DefaultLockService.class).region(REGION_1).lastRebalancedTopology();

                topologies.add(topology);
            });

            return topologies.size() == 1;
        });
    }
}
