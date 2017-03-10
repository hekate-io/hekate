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

package io.hekate.lock.internal;

import io.hekate.HekateInstanceContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopologyHash;
import io.hekate.core.HekateTestInstance;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockServiceFactory;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class LockServiceTestBase extends HekateInstanceContextTestBase {
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

    protected HekateTestInstance createInstanceWithLockService() throws Exception {
        return createInstanceWithLockService(null);
    }

    protected HekateTestInstance createInstanceWithLockService(Configurer configurer) throws Exception {
        return createInstance(c -> {
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

    protected HekateTestInstance awaitForQueuedLock(String lock, HekateTestInstance owner, List<HekateTestInstance> nodes)
        throws Exception {
        AtomicReference<HekateTestInstance> result = new AtomicReference<>();

        busyWait("queued lock [lock=" + lock + ']', () -> {
            for (HekateTestInstance node : nodes) {
                List<ClusterNodeId> locks = node.get(DefaultLockService.class).get(REGION_1).getQueuedLocks(lock);

                if (locks.contains(owner.getNode().getId())) {
                    result.set(node);

                    return true;
                }
            }

            return false;
        });

        assertNotNull(result.get());

        return result.get();
    }

    protected String getRemotelyManagedLockName(HekateTestInstance node) throws Exception {
        return getRemotelyManagedLockName("test", node);
    }

    protected String getRemotelyManagedLockName(String lockPrefix, HekateTestInstance node) throws Exception {
        DefaultLockRegion region = node.get(DefaultLockService.class).get(REGION_1);

        for (int i = 0; i < BUSY_WAIT_LOOPS; i++) {
            String lockName = lockPrefix + i;

            if (!region.getLockManagerNode(lockName).equals(node.getNode().getId())) {
                return lockName;
            }

            Thread.sleep(BUSY_WAIT_INTERVAL);
        }

        throw new AssertionError("Failed to find remotely managed lock.");
    }

    protected HekateTestInstance getLockManagerNode(String lock, List<HekateTestInstance> nodes) {
        assertTrue(nodes.size() > 1);

        ClusterNodeId nodeId = nodes.get(0).get(DefaultLockService.class).get(REGION_1).getLockManagerNode(lock);

        return nodes.stream()
            .filter(n -> n.getNode().getId().equals(nodeId))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("Failed to find lock manager node [lock=" + lock + ']'));
    }

    protected void awaitForLockTopology(List<HekateTestInstance> nodes) throws Exception {
        busyWait("consistent lock topology", () -> {
            Set<ClusterTopologyHash> topologies = new HashSet<>();

            nodes.forEach(n -> {
                ClusterTopologyHash topology = n.get(DefaultLockService.class).get(REGION_1).getEffectiveTopology();

                topologies.add(topology);
            });

            return topologies.size() == 1;
        });
    }
}
