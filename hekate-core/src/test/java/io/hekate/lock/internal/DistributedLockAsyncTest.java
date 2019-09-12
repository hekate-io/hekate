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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.lock.AsyncLockCallback;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DistributedLockAsyncTest extends LockServiceTestBase {
    private static class TestLockCallback implements AsyncLockCallback {
        private final HekateTestNode node;

        private final ExecutorService worker;

        private final CountDownLatch acquireLatch = new CountDownLatch(1);

        private final CountDownLatch busyLatch = new CountDownLatch(1);

        private final CountDownLatch releaseLatch = new CountDownLatch(1);

        private final List<LockOwnerInfo> owners = new ArrayList<>();

        public TestLockCallback(HekateTestNode node, ExecutorService worker) {
            this.node = node;
            this.worker = worker;
        }

        @Override
        public void onLockAcquire(DistributedLock lock) {
            say("Lock acquired by " + node.localNode());

            acquireLatch.countDown();
        }

        @Override
        public synchronized void onLockBusy(LockOwnerInfo owner) {
            say("Lock is busy on " + node.localNode() + " (owner=" + owner + ')');

            owners.add(owner);

            busyLatch.countDown();
        }

        @Override
        public synchronized void onLockOwnerChange(LockOwnerInfo owner) {
            say("Lock owner change on " + node.localNode() + " (owner=" + owner + ')');

            owners.add(owner);
        }

        @Override
        public void onLockRelease(DistributedLock lock) {
            say("Lock released on " + node.localNode());

            releaseLatch.countDown();
        }

        public void ensureUnlocked(String lockRegion, String lockName) {
            AtomicReference<AssertionError> errRef = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);

            worker.execute(() -> {
                try {
                    DistributedLock lock = node.locks().region(lockRegion).get(lockName);

                    if (lock.isHeldByCurrentThread()) {
                        lock.unlock();
                    }
                } catch (RuntimeException | Error e) {
                    errRef.set(new AssertionError(e));
                } finally {
                    done.countDown();
                }
            });

            await(done);

            if (errRef.get() != null) {
                throw errRef.get();
            }
        }

        public void awaitAcquire() {
            say("Awaiting for acquire on " + node.localNode());

            await(acquireLatch);
        }

        public void awaitBusy() {
            say("Awaiting for busy on " + node.localNode());

            await(busyLatch);
        }

        public void awaitRelease() {
            say("Awaiting for release on " + node.localNode());

            await(releaseLatch);
        }

        public void awaitOwnerChange(ClusterNode expected) throws Exception {
            say("Awaiting for lock owner change on " + node.localNode());

            busyWait("lock owner change [expected=" + expected + ", actual=" + getLastOwner() + ']', () -> {
                synchronized (this) {
                    LockOwnerInfo last = getLastOwner();

                    return last != null && last.node().equals(expected);
                }
            });
        }

        public synchronized LockOwnerInfo getLastOwner() {
            return owners.isEmpty() ? null : owners.get(owners.size() - 1);
        }

        public HekateTestNode getNode() {
            return node;
        }

        public ExecutorService getWorker() {
            return worker;
        }
    }

    public DistributedLockAsyncTest(LockTestContext ctx) {
        super(ctx);
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<LockTestContext> getLockAsyncTestContexts() {
        return mapTestContext(p -> Stream.of(
            new LockTestContext(p, 1, 0),
            new LockTestContext(p, 1, 1),
            new LockTestContext(p, 2, 1),
            new LockTestContext(p, 2, 2)
        ));
    }

    @Test
    public void testLock() throws Throwable {
        HekateTestNode node = createLockNode().join();

        DistributedLock lock = node.locks().region(REGION_1).get("test");

        ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("lock-test"));

        AtomicReference<Throwable> errRef = new AtomicReference<>();

        try {
            CountDownLatch lockedLatch = new CountDownLatch(1);
            CountDownLatch unlockLatch = new CountDownLatch(1);
            CountDownLatch unlockedLatch = new CountDownLatch(1);

            lock.lockAsync(worker, locked -> {
                try {
                    assertTrue(locked.isHeldByCurrentThread());

                    lockedLatch.countDown();

                    await(unlockLatch);

                    locked.unlock();

                    unlockedLatch.countDown();
                } catch (Throwable t) {
                    errRef.set(t);
                }
            });

            // Await for lock.
            await(lockedLatch);

            // Must be locked by the async executor thread.
            assertFalse(lock.tryLock());

            // Notify to unlock.
            unlockLatch.countDown();

            // Await for unlock.
            await(unlockedLatch);
        } finally {
            worker.shutdownNow();

            assertTrue(worker.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS));
        }

        if (errRef.get() != null) {
            throw errRef.get();
        }
    }

    @Test
    public void testReleaseOnNodeTermination() throws Throwable {
        HekateTestNode node = createLockNode().join();

        DistributedLock lock = node.locks().region(REGION_1).get("test");

        ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("lock-test"));

        AtomicReference<Throwable> errRef = new AtomicReference<>();

        try {
            CountDownLatch lockedLatch = new CountDownLatch(1);
            CountDownLatch releasedLatch = new CountDownLatch(1);

            lock.lockAsync(worker, new AsyncLockCallback() {
                @Override
                public void onLockAcquire(DistributedLock locked) {
                    lockedLatch.countDown();
                }

                @Override
                public void onLockRelease(DistributedLock lock) {
                    try {
                        assertEquals(1, releasedLatch.getCount());
                        assertFalse(lock.isHeldByCurrentThread());
                    } catch (Throwable t) {
                        errRef.set(t);
                    } finally {
                        releasedLatch.countDown();
                    }
                }
            });

            // Await for lock.
            await(lockedLatch);

            // Must be locked by the async executor thread.
            assertFalse(lock.tryLock());

            node.leave();

            // Check that release method was notified.
            await(releasedLatch);
        } finally {
            worker.shutdownNow();

            assertTrue(worker.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS));
        }

        if (errRef.get() != null) {
            throw errRef.get();
        }
    }

    @Test
    public void testLockBusy() throws Throwable {
        HekateTestNode ownerNode = createLockNode().join();
        HekateTestNode waitingNode = createLockNode().join();

        DistributedLock ownerLock = ownerNode.locks().region(REGION_1).get("test");
        DistributedLock waitingLock = waitingNode.locks().region(REGION_1).get("test");

        ownerLock.lock();

        try {
            ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("lock-test"));

            AtomicReference<Throwable> errRef = new AtomicReference<>();

            try {
                CountDownLatch lockBusyLatch = new CountDownLatch(1);
                CountDownLatch unlockedLatch = new CountDownLatch(1);
                AtomicReference<LockOwnerInfo> lockBusyRef = new AtomicReference<>();
                AtomicBoolean lockOwnerChanged = new AtomicBoolean();
                CountDownLatch releasedLatch = new CountDownLatch(1);

                waitingLock.lockAsync(worker, new AsyncLockCallback() {
                    @Override
                    public void onLockBusy(LockOwnerInfo owner) {
                        try {
                            assertNull(lockBusyRef.get());

                            lockBusyRef.set(owner);

                            lockBusyLatch.countDown();
                        } catch (Throwable t) {
                            errRef.set(t);
                        }
                    }

                    @Override
                    public void onLockOwnerChange(LockOwnerInfo owner) {
                        lockOwnerChanged.set(true);
                    }

                    @Override
                    public void onLockAcquire(DistributedLock locked) {
                        try {
                            locked.unlock();

                            unlockedLatch.countDown();
                        } catch (Throwable t) {
                            errRef.set(t);
                        }
                    }

                    @Override
                    public void onLockRelease(DistributedLock lock) {
                        try {
                            assertEquals(1, releasedLatch.getCount());
                        } catch (Throwable t) {
                            errRef.set(t);
                        } finally {
                            releasedLatch.countDown();
                        }
                    }
                });

                // Await for lock busy notification.
                await(lockBusyLatch);

                assertNotNull(lockBusyRef.get());
                assertEquals(ownerNode.localNode(), lockBusyRef.get().node());
                assertEquals(Thread.currentThread().getId(), lockBusyRef.get().threadId());

                // Stop owner node in order to release lock.
                ownerNode.leave();

                // Await for unlock which means that lock was acquired.
                await(unlockedLatch);

                // Check that there was no owner change notification.
                assertFalse(lockOwnerChanged.get());

                // Check that release method was notified.
                await(releasedLatch);
            } finally {
                worker.shutdownNow();

                assertTrue(worker.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS));
            }

            if (errRef.get() != null) {
                throw errRef.get();
            }
        } finally {
            if (ownerLock.isHeldByCurrentThread()) {
                ownerLock.unlock();
            }
        }
    }

    @Test
    public void testLockOwnerChange() throws Throwable {
        List<ExecutorService> workers = new ArrayList<>();
        List<TestLockCallback> nodeCallbacks = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("lock-test-" + i));

            workers.add(worker);

            HekateTestNode node = createLockNode().join();

            TestLockCallback callback = new TestLockCallback(node, worker);

            nodeCallbacks.add(callback);
        }

        awaitForLockTopology(nodeCallbacks.stream().map(TestLockCallback::getNode).collect(toList()));

        try {
            repeat(nodeCallbacks.size(), i -> {
                TestLockCallback callback = nodeCallbacks.get(i);

                HekateTestNode node = callback.getNode();

                DistributedLock lock = node.locks().region(REGION_1).get("test");

                lock.lockAsync(callback.getWorker(), callback);

                if (i == 0) {
                    // For initial node await for lock acquisition.
                    callback.awaitAcquire();
                } else {
                    // For other nodes await for their locks to be enqueued.
                    callback.awaitBusy();
                }

                if (i >= 3) {
                    int ownerIdx = i - 3;

                    // When there are 4 nodes (A, B, C, D) with locks then:
                    // - A is the lock owner
                    // - B, C and D are in the locking queue.
                    TestLockCallback prevLockOwner = nodeCallbacks.get(ownerIdx);

                    ClusterNode prevLockOwnerNode = prevLockOwner.getNode().localNode();

                    // Release A lock.
                    // This will make B the lock owner (since it was the first node in the queue before A stopped).
                    prevLockOwner.ensureUnlocked(REGION_1, "test");

                    // Await for A to release its lock.
                    prevLockOwner.awaitRelease();

                    TestLockCallback newLockOwner = null;

                    for (int j = ownerIdx + 1; j <= i; j++) {
                        TestLockCallback nodeCallback = nodeCallbacks.get(j);

                        if (newLockOwner == null) {
                            newLockOwner = nodeCallback;

                            // Check that B acquired lock.
                            newLockOwner.awaitAcquire();

                            // Check that there was no redundant notification of lock owner change (except for the very first lock).
                            if (ownerIdx > 0) {
                                assertEquals(prevLockOwnerNode, newLockOwner.getLastOwner().node());
                            }
                        } else {
                            // Check that C and D got lock owner change notification.
                            nodeCallback.awaitOwnerChange(newLockOwner.getNode().localNode());
                        }
                    }
                }
            });

            say("Done.");

            for (TestLockCallback callback : nodeCallbacks) {
                callback.ensureUnlocked(REGION_1, "test");

                callback.getNode().leave();
            }
        } finally {
            workers.forEach(ExecutorService::shutdown);

            for (ExecutorService worker : workers) {
                assertTrue(worker.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void testLockFuture() throws Throwable {
        HekateTestNode node1 = createLockNode().join();
        HekateTestNode node2 = createLockNode().join();
        HekateTestNode node3 = createLockNode().join();

        ExecutorService worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("test-lock"));

        DistributedLock lock1 = node1.locks().region(REGION_1).get("test");
        DistributedLock lock2 = node2.locks().region(REGION_1).get("test");
        DistributedLock lock3 = node3.locks().region(REGION_1).get("test");

        // Obtain initial lock.
        lock1.lock();

        try {
            // Enqueue first pending lock.
            TestLockCallback callback2 = new TestLockCallback(node2, worker);

            Future<?> future2 = lock2.lockAsync(worker, callback2);

            assertFalse(future2.isDone());

            callback2.awaitBusy();

            assertFalse(future2.isDone());

            // Enqueue second pending lock.
            TestLockCallback callback3 = new TestLockCallback(node3, worker);

            Future<?> future3 = lock3.lockAsync(worker, callback3);

            callback3.awaitBusy();

            // Cancel first pending lock.
            future2.cancel(true);

            // Unlock initial lock.
            lock1.unlock();

            // Make sure that second pending lock was obtained (since first lock was cancelled).
            get(future3);

            callback3.awaitAcquire();

            callback3.ensureUnlocked(REGION_1, "test");
        } finally {
            if (lock1.isHeldByCurrentThread()) {
                lock1.unlock();
            }

            node1.leave();
            node2.leave();
            node3.leave();

            worker.shutdown();

            assertTrue(worker.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS));
        }
    }
}
