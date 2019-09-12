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

import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.Hekate;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.lock.LockRegion;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DistributedLockTest extends LockServiceTestBase {
    private interface AsyncLockTask {
        void execute(CountDownLatch latch) throws Exception;
    }

    public static class DistributedLockTestContext extends LockTestContext {
        private final int nodeCount;

        public DistributedLockTestContext(HekateTestContext src, int nodeCount, int workerThreads, int nioThreads) {
            super(src, workerThreads, nioThreads);

            this.nodeCount = nodeCount;
        }
    }

    public static final int LOCKS_PER_TEST = 10;

    private final int nodeCount;

    private LockRegion region;

    private List<HekateTestNode> nodes;

    private HekateTestNode lockNode;

    public DistributedLockTest(DistributedLockTestContext ctx) {
        super(ctx);

        this.nodeCount = ctx.nodeCount;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<DistributedLockTestContext> getDistributedLockTestContexts() {
        return mapTestContext(p -> Stream.of(
            new DistributedLockTestContext(p, 1, 1, 0),
            new DistributedLockTestContext(p, 1, 2, 2),
            new DistributedLockTestContext(p, 2, 1, 0),
            new DistributedLockTestContext(p, 2, 2, 2),
            new DistributedLockTestContext(p, 3, 1, 0),
            new DistributedLockTestContext(p, 3, 2, 2),
            new DistributedLockTestContext(p, 4, 1, 0),
            new DistributedLockTestContext(p, 4, 2, 2)
        ));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        nodes = new ArrayList<>();

        for (int i = 0; i < nodeCount; i++) {
            HekateTestNode node = createLockNode().join();

            nodes.add(node);
        }

        awaitForTopology(nodes);

        lockNode = nodes.get(0);

        region = lockNode.locks().region(REGION_1);
    }

    @Test
    public void testCluster() throws Exception {
        List<ClusterView> clusters = nodes.stream()
            .map(n -> n.locks().region(REGION_1).cluster())
            .collect(toList());

        clusters.forEach(cluster1 ->
            clusters.forEach(cluster2 ->
                assertEquals(cluster1.topology(), cluster2.topology())
            )
        );

        HekateTestNode newNode = createLockNode().join();

        clusters.forEach(cluster ->
            assertTrue(cluster.awaitFor(topology -> topology.contains(newNode.localNode())))
        );

        CountDownLatch allAwareOfLeave = new CountDownLatch(nodes.size());

        clusters.forEach(cluster -> cluster.addListener(evt -> {
            if (evt.type() == ClusterEventType.CHANGE && evt.asChange().removed().contains(newNode.localNode())) {
                allAwareOfLeave.countDown();
            }
        }));

        newNode.leave();

        await(allAwareOfLeave);
    }

    @Test
    public void testUnlockAfterOwnerLeaves() throws Exception {
        DistributedLock lock = region.get("test");

        lock.lock();

        lockNode.leave();

        lock.unlock();
    }

    @Test
    public void testLockUnlockSameInstance() throws Exception {
        repeat(LOCKS_PER_TEST, i -> {
            DistributedLock lock = region.get("test" + i);

            assertEquals(REGION_1, lock.regionName());
            assertEquals("test" + i, lock.name());
            assertFalse(lock.isHeldByCurrentThread());
            assertEquals(0, lock.holdCount());

            repeat(3, j -> {
                int locks = j + 1;

                for (int k = 0; k < locks; k++) { // <- reentrant.
                    lock.lock();

                    assertTrue(lock.isHeldByCurrentThread());
                    assertEquals(k + 1, lock.holdCount());
                }

                for (int k = 0; k < locks; k++) { // <- reentrant.
                    assertTrue(lock.isHeldByCurrentThread());
                    assertEquals(locks - k, lock.holdCount());

                    lock.unlock();
                }
            });
        });
    }

    @Test
    public void testLockUnlockAsyncSameInstance() throws Exception {
        repeat(LOCKS_PER_TEST, i -> {
            DistributedLock lock = region.get("test" + i);

            assertEquals(REGION_1, lock.regionName());
            assertEquals("test" + i, lock.name());
            assertFalse(lock.isHeldByCurrentThread());
            assertEquals(0, lock.holdCount());

            repeat(3, j -> {
                int locks = j + 1;

                for (int k = 0; k < locks; k++) { // <- reentrant.
                    lock.lock();

                    assertTrue(lock.isHeldByCurrentThread());
                    assertEquals(k + 1, lock.holdCount());
                }

                for (int k = 0; k < locks; k++) { // <- reentrant.
                    assertTrue(lock.isHeldByCurrentThread());
                    assertEquals(locks - k, lock.holdCount());

                    lock.unlockAsync();
                }
            });
        });
    }

    @Test
    public void testLockUnlockDifferentInstances() throws Exception {
        repeat(LOCKS_PER_TEST, i -> repeat(3, j -> {
            int locks = j + 1;

            for (int k = 0; k < locks; k++) { // <- reentrant.
                String lock = "test" + i;

                region.get(lock).lock();

                assertTrue(region.get(lock).isHeldByCurrentThread());
                assertEquals(k + 1, region.get(lock).holdCount());
            }

            for (int k = 0; k < locks; k++) { // <- reentrant.
                String lock = "test" + i;

                assertTrue(region.get(lock).isHeldByCurrentThread());
                assertEquals(locks - k, region.get(lock).holdCount());

                region.get(lock).unlock();
            }
        }));
    }

    @Test
    public void testLockUnlockAsyncDifferentInstances() throws Exception {
        repeat(LOCKS_PER_TEST, i -> repeat(3, j -> {
            int locks = j + 1;

            for (int k = 0; k < locks; k++) { // <- reentrant.
                String lock = "test" + i;

                region.get(lock).lock();

                assertTrue(region.get(lock).isHeldByCurrentThread());
                assertEquals(k + 1, region.get(lock).holdCount());
            }

            for (int k = 0; k < locks; k++) { // <- reentrant.
                String lock = "test" + i;

                assertTrue(region.get(lock).isHeldByCurrentThread());
                assertEquals(locks - k, region.get(lock).holdCount());

                region.get(lock).unlockAsync();
            }
        }));
    }

    @Test
    public void testLockInterruptibly() throws Exception {
        repeat(LOCKS_PER_TEST, i -> {
            DistributedLock lock = region.get("test" + i);

            repeat(3, j -> {
                try {
                    CountDownLatch asyncLock = new CountDownLatch(1);
                    CountDownLatch asyncUnlock = new CountDownLatch(1);

                    // Need to make sure that lock is held by another thread,
                    // otherwise lockInterruptibly() can succeed w/o throwing InterruptedException.
                    // This happens if lock gets successfully acquired before lockInterruptibly() starts waiting.
                    Future<Object> async = asyncStagedLock(lock, asyncLock, asyncUnlock);

                    // Await for lock to be acquired by another thread.
                    await(asyncLock);

                    // Force interruption of the current thread.
                    Thread.currentThread().interrupt();

                    try {
                        lock.lockInterruptibly();

                        // Make sure that lock is released even if test fails.
                        lock.unlock();

                        fail("Error was expected.");
                    } catch (InterruptedException e) {
                        // Expected.
                    }

                    // Notify asynchronous unlock.
                    asyncUnlock.countDown();

                    // Await for unlock.
                    get(async);

                    assertFalse(Thread.currentThread().isInterrupted());

                    // Check that other tread can acquire interrupted lock.
                    checkCanLockAsync(lock);

                    // Check that lock can be acquired if interrupted flag is reset.
                    lock.lockInterruptibly();

                    lock.unlock();
                } finally {
                    if (Thread.currentThread().isInterrupted()) {
                        Thread.interrupted();
                    }
                }
            });
        });
    }

    @Test
    public void testTryLockWithTimeoutInterruptibly() throws Exception {
        repeat(LOCKS_PER_TEST, i -> {
            DistributedLock lock = region.get("test" + i);

            repeat(3, j -> {
                try {
                    CountDownLatch asyncLock = new CountDownLatch(1);
                    CountDownLatch asyncUnlock = new CountDownLatch(1);

                    // Need to make sure that lock is held by another thread,
                    // otherwise lockInterruptibly() can succeed w/o throwing InterruptedException.
                    // This happens if lock gets successfully acquired before lockInterruptibly() starts waiting.
                    Future<Object> async = asyncStagedLock(lock, asyncLock, asyncUnlock);

                    // Await for lock to be acquired by another thread.
                    await(asyncLock);

                    // Force interruption of the current thread.
                    Thread.currentThread().interrupt();

                    try {
                        lock.tryLock(AWAIT_TIMEOUT, TimeUnit.SECONDS);

                        // Make sure that lock is released even if test fails.
                        lock.unlock();

                        fail("Error was expected.");
                    } catch (InterruptedException e) {
                        // Expected.
                    }

                    // Notify asynchronous unlock.
                    asyncUnlock.countDown();

                    // Await for unlock.
                    get(async);

                    assertFalse(Thread.currentThread().isInterrupted());

                    // Check that other tread can acquire interrupted lock.
                    checkCanLockAsync(lock);

                    // Check that lock can be acquired if interrupted flag is reset.
                    assertTrue(lock.tryLock(AWAIT_TIMEOUT, TimeUnit.SECONDS));

                    lock.unlock();
                } finally {
                    if (Thread.currentThread().isInterrupted()) {
                        Thread.interrupted();
                    }
                }
            });
        });
    }

    @Test
    public void testTryFreeLockWithTimeout() throws Exception {
        repeat(LOCKS_PER_TEST, i -> {
            DistributedLock lock = region.get("test" + i);

            repeat(3, j -> {
                for (int k = 0; k < j + 1; k++) { // <- reentrant.
                    // Small timeout since locking doesn't take communication overhead into account.
                    assertTrue(lock.tryLock(1, TimeUnit.NANOSECONDS));
                }

                for (int k = 0; k < j + 1; k++) { // <- reentrant.
                    lock.unlock();
                }
            });
        });
    }

    @Test
    public void testTryFreeLock() throws Exception {
        repeat(LOCKS_PER_TEST, i -> {
            DistributedLock lock = region.get("test" + i);

            repeat(3, j -> {
                for (int k = 0; k < j + 1; k++) { // <- reentrant.
                    assertTrue(lock.tryLock());
                }

                for (int k = 0; k < j + 1; k++) { // <- reentrant.
                    lock.unlock();
                }
            });
        });
    }

    @Test
    public void testTryLockWithTerminatedService() throws Exception {
        DistributedLock lock = region.get("test");

        lockNode.leave();

        assertFalse(lock.tryLock());
        assertFalse(lock.tryLock(0, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testTryBusyLock() throws Exception {
        repeat(LOCKS_PER_TEST, i -> runWithBusyLock("test" + i, latch -> {
            DistributedLock lock = region.get("test" + i);

            repeat(3, k -> assertFalse(lock.tryLock()));
        }));
    }

    @Test
    public void testTryBusyLockWithTimeout() throws Exception {
        repeat(LOCKS_PER_TEST, i -> runWithBusyLock("test" + i, latch -> {
            DistributedLock lock = region.get("test" + i);

            // Small timeout since locking doesn't take communication overhead into account.
            repeat(3, k -> assertFalse(lock.tryLock(1, TimeUnit.NANOSECONDS)));
        }));
    }

    @Test
    public void testNonReentrantWithDifferentNodes() throws Exception {
        repeat(LOCKS_PER_TEST, i -> repeat(3, j -> nodes.stream()
            .filter(node -> !node.localNode().equals(lockNode.localNode()))
            .forEach(node -> {
                region.get("test" + i).lock();

                try {
                    DistributedLock otherLock = node.locks().region(REGION_1).get("test" + i);

                    if (otherLock.tryLock()) {
                        otherLock.unlock();

                        fail("Got unexpected lock.");
                    }
                } finally {
                    region.get("test" + i).unlock();
                }
            })));
    }

    @Test
    public void testLockAfterReleaseByOtherNode() throws Exception {
        repeat(LOCKS_PER_TEST, i -> runWithBusyLock("test" + i, asyncUnlock -> {
            String lockName = "test" + i;

            DistributedLock lock = region.get(lockName);

            Future<Object> async = runAsync(() -> {
                awaitForQueuedLock(lockName);

                // Notify lock owner to release the lock.
                asyncUnlock.countDown();

                return null;
            });

            lockUnlock(lock);

            get(async);
        }));
    }

    @Test
    public void testLockWithTimeoutAfterReleaseByOtherNode() throws Exception {
        repeat(LOCKS_PER_TEST, i -> runWithBusyLock("test" + i, asyncUnlock -> {
            String lockName = "test" + i;

            DistributedLock lock = region.get(lockName);

            Future<Object> async = runAsync(() -> {
                awaitForQueuedLock(lockName);

                // Notify lock owner to release the lock.
                asyncUnlock.countDown();

                return null;
            });

            assertTrue(lock.tryLock(AWAIT_TIMEOUT, TimeUnit.SECONDS));

            lock.unlock();

            get(async);
        }));
    }

    @Test
    public void testLockAfterManagerLeave() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            String lockName = getRemotelyManagedLockName();

            HekateTestNode managerNode = getLockManagerNode(lockName);

            runWithBusyLock(lockName, managerNode, asyncUnlock -> {
                DistributedLock lock = region.get(lockName);

                // Asynchronously await for lock to be placed on the queue and leave node.
                Future<Object> async = runAsync(() -> {
                    awaitForQueuedLock(lockName);

                    managerNode.leave();

                    return null;
                });

                lockUnlock(lock);

                get(async);

                asyncUnlock.countDown();
            });
        }
    }

    @Test
    public void testLockAfterManagerTermination() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            disableNodeFailurePostCheck();

            String lockName = getRemotelyManagedLockName();

            HekateTestNode managerNode = getLockManagerNode(lockName);

            // Asynchronously await for lock to be placed on the queue and terminate node.
            runWithBusyLock(lockName, managerNode, asyncUnlock -> {
                DistributedLock lock = region.get(lockName);

                Future<Object> async = runAsync(() -> {
                    awaitForQueuedLock(lockName);

                    say("Terminating %s...", managerNode);

                    managerNode.terminate();

                    say("Terminated manager.");

                    return null;
                });

                lockUnlock(lock);

                get(async);

                asyncUnlock.countDown();
            });
        }
    }

    @Test
    public void testLockWithTimeoutAfterManagerLeave() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            String lockName = getRemotelyManagedLockName();

            HekateTestNode managerNode = getLockManagerNode(lockName);

            runWithBusyLock(lockName, managerNode, asyncUnlock -> {
                DistributedLock lock = region.get(lockName);

                // Asynchronously await for lock to be placed on the queue and leave node.
                Future<Object> async = runAsync(() -> {
                    awaitForQueuedLock(lockName);

                    managerNode.leave();

                    return null;
                });

                assertTrue(lock.tryLock(AWAIT_TIMEOUT, TimeUnit.SECONDS));

                lock.unlock();

                get(async);

                asyncUnlock.countDown();
            });
        }
    }

    @Test
    public void testLockWithTimeoutAfterManagerTermination() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            disableNodeFailurePostCheck();

            String lockName = getRemotelyManagedLockName();

            HekateTestNode managerNode = getLockManagerNode(lockName);

            runWithBusyLock(lockName, managerNode, asyncUnlock -> {
                DistributedLock lock = region.get(lockName);

                // Asynchronously await for lock to be placed on the queue and terminate node.
                Future<Object> async = runAsync(() -> {
                    awaitForQueuedLock(lockName);

                    say("Terminating %s...", managerNode);

                    managerNode.terminate();

                    say("Terminated.");

                    return null;
                });

                say("Locking....");

                assertTrue(lock.tryLock(AWAIT_TIMEOUT, TimeUnit.SECONDS));

                say("Locked.");

                lock.unlock();

                say("Unlocked.");

                get(async);

                asyncUnlock.countDown();
            });
        }
    }

    @Test
    public void testLockAfterQueuedNodeLeave() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            String lockName = getRemotelyManagedLockName();

            HekateTestNode managerNode = getLockManagerNode(lockName);

            runWithBusyLock(lockName, managerNode, asyncUnlock -> {
                DistributedLock lock = region.get(lockName);

                Future<Object> async = runAsync(() -> {
                    awaitForQueuedLock(lockName);

                    // Leave the node that is trying to acquire the lock.
                    lockNode.leave();

                    return null;
                });

                try {
                    lock.lock();

                    fail("Failure was expected.");
                } catch (CancellationException e) {
                    assertEquals("Lock service terminated.", e.getMessage());
                }

                get(async);

                asyncUnlock.countDown();
            });

            DistributedLock lock = managerNode.locks().region(REGION_1).get(lockName);

            lockUnlock(lock);
        }
    }

    @Test
    public void testLockAfterQueuedNodeTermination() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            disableNodeFailurePostCheck();

            String lockName = getRemotelyManagedLockName();

            HekateTestNode managerNode = getLockManagerNode(lockName);

            runWithBusyLock(lockName, managerNode, asyncUnlock -> {
                DistributedLock lock = region.get(lockName);

                Future<Object> async = runAsync(() -> {
                    awaitForQueuedLock(lockName);

                    // Terminate the node that is trying to acquire the lock.
                    lockNode.terminate();

                    return null;
                });

                try {
                    lock.lock();

                    fail("Failure was expected.");
                } catch (CancellationException e) {
                    assertEquals("Lock service terminated.", e.getMessage());
                }

                get(async);

                asyncUnlock.countDown();
            });

            DistributedLock lock = managerNode.locks().region(REGION_1).get(lockName);

            lockUnlock(lock);
        }
    }

    @Test
    public void testLockLivenessIfAtLeastOneNodeExists() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            for (int i = 0; i < 100; i++) {
                region.get("test" + i).lock();
            }

            try {
                List<HekateTestNode> nodesCopy = new ArrayList<>(nodes);

                for (HekateTestNode node : nodes) {
                    if (node.localNode().equals(lockNode.localNode())) {
                        // Skip lock owner.
                        continue;
                    }

                    node.leave();

                    nodesCopy.remove(node);

                    awaitForTopology(nodesCopy);

                    awaitForLockTopology(nodesCopy);

                    // Try to lock asynchronously (should fail since lock is held by the main thread).
                    boolean asyncLocked = runAsyncAndGet(() -> {
                        for (int i = 0; i < 100; i++) {
                            if (region.get("test" + i).tryLock()) {
                                region.get("test" + i).unlock();

                                return true;
                            }
                        }

                        return false;
                    });

                    assertFalse(asyncLocked);
                }
            } finally {
                for (int i = 0; i < 100; i++) {
                    region.get("test" + i).unlock();
                }
            }

            // Double check that we can still lock/unlock after all cluster changes.
            for (int i = 0; i < 100; i++) {
                region.get("test" + i).lock();
            }

            for (int i = 0; i < 100; i++) {
                region.get("test" + i).unlock();
            }
        }
    }

    @Test
    public void testQueuedLockAfterOtherLockTimeout() throws Exception {
        String lockName = "test";

        runWithBusyLock(lockName, latch -> {
            DistributedLock lock = region.get(lockName);

            // Schedule asynchronous lock so that it would be queued after the lock with timeout.
            Future<Boolean> async = runAsync(() -> {
                awaitForQueuedLock(lockName);

                DistributedLock otherLock = region.get(lockName);

                lockUnlock(otherLock);

                return null;
            });

            // Queue lock with timeout (should fail since main lock is held).
            if (lock.tryLock(BUSY_WAIT_INTERVAL * 10, TimeUnit.MILLISECONDS)) {
                lock.unlock();

                fail("Unexpected lock.");
            }

            // Release lock.
            latch.countDown();

            // Make sure that asynchronous lock was successfully locked and unlocked.
            async.get();
        });
    }

    @Test
    public void testConcurrentLockFromSameNode() throws Exception {
        AtomicBoolean locked = new AtomicBoolean();

        DistributedLock lock = region.get("test");

        runParallel(5, 50, i -> {
            lock.lock();

            assertTrue(locked.compareAndSet(false, true));

            sleep(1);

            locked.set(false);

            lock.unlock();
        });
    }

    @Test
    public void testConcurrentLockFromDifferentNodes() throws Exception {
        if (nodes.size() > 1) { // <-- skip test if running only with a single node.
            AtomicBoolean locked = new AtomicBoolean();

            runParallel(nodes.size(), 50, i -> {
                DistributedLock lock = nodes.get(i.getThread()).locks().region(REGION_1).get("test");

                lock.lock();

                assertTrue(locked.compareAndSet(false, true));

                sleep(5);

                locked.set(false);

                lock.unlock();
            });
        }
    }

    @Test
    public void testGetOwnerOfNonExistingLock() throws Exception {
        assertFalse(region.ownerOf("no_such_lock").isPresent());
        assertFalse(region.get("no_such_lock").owner().isPresent());
    }

    @Test
    public void testGetOwner() throws Exception {
        region.get("test").lock();

        long threadId = Thread.currentThread().getId();

        try {
            nodes.stream()
                .map(node -> node.locks().region(region.name()))
                .forEach(region -> {
                    try {
                        LockOwnerInfo fromRegion = region.ownerOf("test").get();

                        assertEquals(lockNode.localNode(), fromRegion.node());
                        assertEquals(threadId, fromRegion.threadId());

                        LockOwnerInfo fromLock = region.get("test").owner().get();

                        assertEquals(lockNode.localNode(), fromLock.node());
                        assertEquals(threadId, fromLock.threadId());
                    } catch (InterruptedException e) {
                        throw new AssertionError("Thread was unexpectedly interrupted.", e);
                    }
                });

        } finally {
            region.get("test").unlock();
        }
    }

    @Test
    public void testGetOwnerTerminatedService() throws Exception {
        DistributedLock lock = region.get("test");

        lock.lock();

        lockNode.leave();

        assertFalse(lock.owner().isPresent());
    }

    private void lockUnlock(DistributedLock lock) {
        lock.lock();

        lock.unlock();
    }

    private HekateTestNode awaitForQueuedLock(String lock) throws Exception {
        return awaitForQueuedLock(lock, lockNode, nodes);
    }

    private String getRemotelyManagedLockName() throws Exception {
        assertTrue(nodes.size() > 1);

        return getRemotelyManagedLockName(lockNode);
    }

    private HekateTestNode getLockManagerNode(String lock) {
        return getLockManagerNode(lock, nodes);
    }

    private void runWithBusyLock(String lockName, AsyncLockTask task) throws Exception {
        for (HekateTestNode node : nodes) {
            runWithBusyLock(lockName, node, task);
        }
    }

    private void runWithBusyLock(String lockName, HekateTestNode node, AsyncLockTask task) throws Exception {
        CountDownLatch lockAsync = new CountDownLatch(1);
        CountDownLatch unlockAsync = new CountDownLatch(1);

        Future<Object> async = runAsync(() -> {
            DistributedLock otherLock = node.locks().region(REGION_1).get(lockName);

            otherLock.lock();

            try {
                lockAsync.countDown();

                await(unlockAsync);
            } finally {
                otherLock.unlock();
            }

            return null;
        });

        await(lockAsync);

        task.execute(unlockAsync);

        unlockAsync.countDown();

        get(async);

        if (node.state() == Hekate.State.UP) {
            // Double check that lock can be still be acquired after task is finished.
            DistributedLock lock = node.locks().region(REGION_1).get(lockName);

            lockUnlock(lock);
        }
    }

    private void checkCanLockAsync(DistributedLock lock) throws InterruptedException, ExecutionException, TimeoutException {
        assertTrue(get(runAsync(() -> {
            if (lock.tryLock()) {
                lock.unlock();

                return true;
            }

            return false;
        })));
    }

    private Future<Object> asyncStagedLock(DistributedLock lock, CountDownLatch asyncLock, CountDownLatch asyncUnlock) {
        return runAsync(() -> {
            lock.lock();

            asyncLock.countDown();

            await(asyncUnlock);

            lock.unlock();

            return null;
        });
    }
}
