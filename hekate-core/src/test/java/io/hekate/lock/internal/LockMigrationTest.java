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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.HekateFutureException;
import io.hekate.core.HekateTestInstance;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.lock.internal.LockProtocol.MigrationApplyRequest;
import io.hekate.lock.internal.LockProtocol.MigrationPrepareRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockMigrationTest extends LockServiceTestBase {
    private final List<HekateTestInstance> nodes = Collections.synchronizedList(new ArrayList<>());

    private final Map<ClusterNode, List<DistributedLock>> liveLocks = Collections.synchronizedMap(new HashMap<>());

    private HekateTestInstance coordinator;

    private HekateTestInstance nextAfterCoordinator;

    private HekateTestInstance nextAfterNext;

    private HekateTestInstance lastNode;

    public LockMigrationTest(LockTestContext ctx) {
        super(ctx);
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<LockTestContext> getLockMigrationTestContexts() {
        return mapTestContext(p -> Stream.of(
            new LockTestContext(p, 1, 0),
            new LockTestContext(p, 1, 1),
            new LockTestContext(p, 2, 1),
            new LockTestContext(p, 2, 2)
        ));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < 5; i++) {
            nodes.add(createInstanceWithLockService().join());
        }

        nodes.sort(Comparator.comparingInt(o -> o.getNode().getJoinOrder()));

        awaitForTopology(nodes);

        coordinator = nodes.get(0);
        nextAfterCoordinator = nodes.get(1);
        nextAfterNext = nodes.get(2);
        lastNode = nodes.get(nodes.size() - 1);

        nodes.forEach(n -> {
            List<DistributedLock> nodeLocks = new ArrayList<>();

            liveLocks.put(n.getNode(), nodeLocks);

            for (int i = 0; i < 50; i++) {
                String lockName = "'" + n.getNode().getJoinOrder() + "-" + i + '\'';

                DistributedLock lockReg1 = n.get(LockService.class).get(REGION_1).getLock(lockName);

                lockReg1.lock();

                nodeLocks.add(lockReg1);
            }

            for (int i = 0; i < 25; i++) {
                String lockName = "'" + n.getNode().getJoinOrder() + "-" + i + '\'';

                DistributedLock lockReg2 = n.get(LockService.class).get(REGION_2).getLock(lockName);

                lockReg2.lock();

                nodeLocks.add(lockReg2);
            }
        });

        awaitForLockTopology(nodes);
    }

    @Override
    public void tearDown() throws Exception {
        liveLocks.values().forEach(nodeLocks -> nodeLocks.stream()
            .filter(DistributedLock::isHeldByCurrentThread)
            .forEach(DistributedLock::unlock));

        liveLocks.clear();

        super.tearDown();
    }

    @Test
    public void testCoordinatorLeaveOnPrepareSent() throws Exception {
        CountDownLatch leaveLatch = new CountDownLatch(1);

        setCallback(coordinator, new LockMigrationCallback() {
            @Override
            public void onAfterPrepareSent(MigrationPrepareRequest request) {
                leaveAsync(coordinator, leaveLatch);
            }
        });

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onPrepareReceived(MigrationPrepareRequest request) {
                await(leaveLatch);
            }
        });

        runMigrationAndCheckLocks(leaveLatch);
    }

    @Test
    public void testCoordinatorLeaveOnPrepareReceived() throws Exception {
        CountDownLatch leaveLatch = new CountDownLatch(1);

        setCallback(coordinator, new LockMigrationCallback() {
            @Override
            public void onPrepareReceived(MigrationPrepareRequest request) {
                leaveAsync(coordinator, leaveLatch);
            }
        });

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onApplyReceived(MigrationApplyRequest request) {
                await(leaveLatch);
            }
        });

        runMigrationAndCheckLocks(leaveLatch);
    }

    @Test
    public void testCoordinatorLeaveOnApplySent() throws Exception {
        CountDownLatch leaveLatch = new CountDownLatch(1);

        setCallback(coordinator, new LockMigrationCallback() {
            @Override
            public void onAfterApplySent(MigrationApplyRequest request) {
                leaveAsync(coordinator, leaveLatch);
            }
        });

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onApplyReceived(MigrationApplyRequest request) {
                await(leaveLatch);
            }
        });

        runMigrationAndCheckLocks(leaveLatch);
    }

    @Test
    public void testNodeLeaveOnPrepare() throws Exception {
        CountDownLatch leaveLatch = new CountDownLatch(1);

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onPrepareReceived(MigrationPrepareRequest request) {
                leaveAsync(nextAfterCoordinator, leaveLatch);
            }
        });

        setCallback(nextAfterNext, new LockMigrationCallback() {
            @Override
            public void onPrepareReceived(MigrationPrepareRequest request) {
                await(leaveLatch);
            }
        });

        runMigrationAndCheckLocks(leaveLatch);
    }

    @Test
    public void testNodeLeaveOnApply() throws Exception {
        CountDownLatch leaveLatch = new CountDownLatch(1);

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onApplyReceived(MigrationApplyRequest request) {
                leaveAsync(nextAfterCoordinator, leaveLatch);
            }
        });

        setCallback(nextAfterNext, new LockMigrationCallback() {
            @Override
            public void onApplyReceived(MigrationApplyRequest request) {
                await(leaveLatch);
            }
        });

        runMigrationAndCheckLocks(leaveLatch);
    }

    @Test
    public void testNodeJoinOnPrepare() throws Exception {
        CountDownLatch joinLatch = new CountDownLatch(1);

        AtomicBoolean joinOnce = new AtomicBoolean();

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onPrepareReceived(MigrationPrepareRequest request) {
                if (joinOnce.compareAndSet(false, true)) {
                    joinAsync(joinLatch);
                }
            }
        });

        setCallback(nextAfterNext, new LockMigrationCallback() {
            @Override
            public void onPrepareReceived(MigrationPrepareRequest request) {
                await(joinLatch);
            }
        });

        runMigrationAndCheckLocks(joinLatch);
    }

    @Test
    public void testNodeJoinOnApply() throws Exception {
        CountDownLatch joinLatch = new CountDownLatch(1);

        AtomicBoolean joinOnce = new AtomicBoolean();

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onApplyReceived(MigrationApplyRequest request) {
                if (joinOnce.compareAndSet(false, true)) {
                    joinAsync(joinLatch);
                }
            }
        });

        setCallback(nextAfterNext, new LockMigrationCallback() {
            @Override
            public void onApplyReceived(MigrationApplyRequest request) {
                await(joinLatch);
            }
        });

        runMigrationAndCheckLocks(joinLatch);
    }

    @Test
    public void testNoMigrationWhenNonLockServiceNodeJoins() throws Exception {
        AtomicBoolean migrated = new AtomicBoolean();

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onAfterPrepareSent(MigrationPrepareRequest request) {
                migrated.set(true);
            }
        });

        HekateTestInstance newNode = createInstance().join();

        awaitForTopology(nodes, newNode);

        awaitForLockTopology(nodes);

        assertFalse(migrated.get());
    }

    @Test
    public void testNoMigrationWhenOtherRegionNodeJoins() throws Exception {
        AtomicBoolean migrated = new AtomicBoolean();

        setCallback(nextAfterCoordinator, new LockMigrationCallback() {
            @Override
            public void onAfterPrepareSent(MigrationPrepareRequest request) {
                migrated.set(true);
            }
        });

        HekateTestInstance newNode = createInstanceWithLockService(c -> {
            c.getRegions().clear();
            c.withRegion(new LockRegionConfig().withName("otherRegion"));
        }).join();

        awaitForTopology(nodes, newNode);

        awaitForLockTopology(nodes);

        assertFalse(migrated.get());

        DistributedLock lock = newNode.get(LockService.class).get("otherRegion").getLock("otherLock");

        assertTrue(lock.tryLock());

        lock.unlock();
    }

    @Test
    public void testMigrationWithQueuedLock() throws Exception {
        CountDownLatch leaveLatch = new CountDownLatch(1);

        DistributedLock existingLock = liveLocks.get(coordinator.getNode()).stream()
            .filter(l -> l.getRegion().equals(REGION_1))
            .findFirst().get();

        // Make sure that existing lock is released.
        existingLock.unlock();

        // Prepare lock.
        DistributedLock lock = nextAfterCoordinator.get(LockService.class).get(REGION_1).getLock(existingLock.getName());

        AtomicBoolean queuedLocked = new AtomicBoolean();
        CountDownLatch lockLatch = new CountDownLatch(1);

        Future<Object> async = runAsync(() -> {
            // Asynchronously lock.
            lock.lock();

            try {
                lockLatch.countDown();

                //  Await for concurrent lock request from another thread.
                awaitForQueuedLock(lock.getName(), nextAfterCoordinator, nodes);

                //  Trigger migration.
                leaveAsync(lastNode, leaveLatch);

                // Await for migration to complete.
                awaitForTopology(nodes);

                awaitForLockTopology(nodes);

                // Make sure that concurrent thread was still unable to acquire lock.
                assertFalse(queuedLocked.get());
            } finally {
                lock.unlock();
            }

            return null;
        });

        // Make sure that lock was asynchronously acquired.
        await(lockLatch);

        runAsyncAndGet(() -> {
            // Asynchronously try to obtain lock while it is held by another thread.
            lock.lock();

            queuedLocked.set(true);

            lock.unlock();

            return null;
        });

        async.get(3, TimeUnit.SECONDS);

        await(leaveLatch);

        awaitForTopology(nodes);

        awaitForLockTopology(nodes);

        checkBusy(lock);

        lock.lock();

        lock.unlock();
    }

    private void runMigrationAndCheckLocks(CountDownLatch latch) throws Exception {
        // Triggers migration.
        leave(lastNode);

        await(latch);

        awaitForTopology(nodes);

        awaitForLockTopology(nodes);

        checkLiveLocksAreBusy();
    }

    private void leave(HekateTestInstance node) throws InterruptedException, HekateFutureException {
        nodes.remove(node);

        node.leave();
    }

    private void leaveAsync(HekateTestInstance node, CountDownLatch leaveLatch) {
        nodes.remove(node);

        node.leaveAsync().thenRun(leaveLatch::countDown);
    }

    private HekateTestInstance joinAsync(CountDownLatch leaveLatch) {
        try {
            HekateTestInstance node = createInstanceWithLockService();

            nodes.add(node);

            node.joinAsync().thenRun(leaveLatch::countDown);

            return node;
        } catch (Exception e) {
            throw new AssertionError("Unexpected exception: " + e, e);
        }
    }

    private void checkLiveLocksAreBusy() throws Exception {
        boolean busy = runAsyncAndGet(() -> nodes.stream()
            .filter(n -> liveLocks.containsKey(n.getNode()))
            .allMatch(n -> liveLocks.get(n.getNode()).stream()
                .allMatch(this::checkBusy)
            ));

        assertTrue(busy);
    }

    private void setCallback(HekateTestInstance node, LockMigrationCallback callback) {
        node.get(DefaultLockService.class).get(REGION_1).setMigrationCallback(callback);
    }

    private boolean checkBusy(DistributedLock lock) {
        if (lock.tryLock()) {
            lock.unlock();

            return false;
        }

        return true;
    }
}
