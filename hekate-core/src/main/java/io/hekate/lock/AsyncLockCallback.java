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

package io.hekate.lock;

import java.util.concurrent.Executor;

/**
 * Callback for asynchronous locking operations.
 *
 * <p>
 * Instances of this interface can be passed to the {@link DistributedLock#lockAsync(Executor, AsyncLockCallback)} method in order to get
 * asynchronous notifications about lock status changes. Method of this interface are executed as follows:
 * </p>
 * <ul>
 * <li>If locking attempt was successful then {@link #onLockAcquire(DistributedLock)} method gets called. The acquired lock will be bound
 * to the context of a callback thread (i.e. {@link DistributedLock#isHeldByCurrentThread()} will return {@code true} if called from the
 * callback). Note that locks are not automatically released and it is up the callback implementation to decide when lock should be {@link
 * DistributedLock#unlock() released}. After lock gets released then the {@link #onLockRelease(DistributedLock)} method will be
 * called.</li>
 * <li>If locking attempt failed because of the lock is being held by some other node/thread then {@link #onLockBusy(LockOwnerInfo)} method
 * will be called and another attempt to acquire the lock will be preform unless the lock gets acquired or operation gets cancelled.</li>
 * <li>If callback is still waiting for the lock and lock owner changes then {@link #onLockOwnerChange(LockOwnerInfo)} method will be
 * notified and another attempt to acquire the lock will be preformed unless lock gets acquired or operation gets cancelled.</li>
 * </ul>
 *
 * @see DistributedLock#lockAsync(Executor, AsyncLockCallback)
 */
public interface AsyncLockCallback {
    /**
     * Gets called when lock was successfully acquired.
     *
     * <p>
     * The acquired lock is {@link DistributedLock#isHeldByCurrentThread() bound} to the thread that calls this method (i.e. {@link
     * DistributedLock#isHeldByCurrentThread()} will return {@code true} if called within this method). Note that lock will NOT be
     * automatically released after this methods finishes its execution. It is up to the implementation to decide when lock should be
     * {@link DistributedLock#unlock() released}.
     * </p>
     *
     * @param lock Lock.
     */
    void onLockAcquire(DistributedLock lock);

    /**
     * Gets called when initial attempt to acquire the lock failed because of the lock is being held by some other node or thread.
     *
     * <p>
     * Note that lock will still remain in a waiting state after this method gets called and once the lock gets acquired then the {@link
     * #onLockAcquire(DistributedLock)} method will be called.
     * </p>
     *
     * @param owner Current lock owner.
     */
    default void onLockBusy(LockOwnerInfo owner) {
        // No-op.
    }

    /**
     * Gets called when lock owner changes while the callback is still waiting for the lock to be acquired.
     *
     * <p>
     * Note that lock will still remain in a waiting state after this method gets called and once the lock gets acquired then the {@link
     * #onLockAcquire(DistributedLock)} method will be called.
     * </p>
     *
     * @param owner New lock owner.
     */
    default void onLockOwnerChange(LockOwnerInfo owner) {
        // No-op.
    }

    /**
     * Gets called after lock has been released. Note that this method gets called only if the lock was previously {@link
     * #onLockAcquire(DistributedLock) acquired}.
     *
     * @param lock Released lock.
     */
    default void onLockRelease(DistributedLock lock) {
        // No-op.
    }
}
