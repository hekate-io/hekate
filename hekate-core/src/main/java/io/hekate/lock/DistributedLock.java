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

import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * Distributed lock.
 *
 * <p>
 * Distributed lock provides support for controlling access to a shared resource or a critical section at the cluster-wide level. At any
 * point in time only one thread on any cluster node can gain a lock with the same {@link #name() name}. All other threads, either running
 * on the same node or on remote nodes, will await for the lock to be released before getting a chance to obtain the lock.
 * </p>
 *
 * <p>
 * Below is the example of how an instance of {@link DistributedLock} can be obtained from the {@link LockService} and used for locking
 * operations:
 * ${source: lock/LockServiceJavadocTest.java#lock}
 * </p>
 *
 * <p>
 * For more details about distributed locks and their usage please see the documentation of {@link LockService} interface.
 * </p>
 *
 * @see LockService
 */
public interface DistributedLock extends Lock {
    /**
     * Returns the name of this lock.
     *
     * @return Name of this lock.
     *
     * @see LockRegion#get(String)
     */
    String name();

    /**
     * Returns the name of the region this lock belongs to.
     *
     * @return Name of the lock region.
     *
     * @see LockService#region(String)
     */
    String regionName();

    /**
     * Returns the number of holds on this lock by the current thread.
     *
     * @return Number of holds on this lock by the current thread.
     */
    int holdCount();

    /**
     * Returns {@code true} if this lock is held by the current thread.
     *
     * @return {@code true} if this lock is held by the current thread.
     */
    boolean isHeldByCurrentThread();

    /**
     * Performs asynchronous locking and notifies the specified callback upon the lock status changes.
     *
     * <p>
     * The specified {@link Executor} instance will be used to perform all notifications of the callback object. Thus it is <b>highly
     * recommended to use a single-threaded executor</b> (f.e. {@link Executors#newSingleThreadExecutor()}) so that all the callback
     * notifications would be performed on the same thread. Otherwise the callback notification order can be completely unpredictable.
     * </p>
     *
     * <p>
     * For details about the callback notification order please see the documentation of {@link AsyncLockCallback} interface.
     * </p>
     *
     * <p>
     * The {@link Future} object, that is returned by this method, can be used to wait for asynchronous lock acquisition (after the lock
     * have been acquired but before the {@link AsyncLockCallback#onLockAcquire(DistributedLock)} method gets notified).
     * </p>
     *
     * @param executor Executor to perform asynchronous notifications of callback methods.
     * @param callback Callback.
     *
     * @return Future object that can be used to await for the lock acquisition.
     */
    Future<?> lockAsync(Executor executor, AsyncLockCallback callback);

    /**
     * Unlocks the lock. If current thread is not the lock owner then {@link IllegalMonitorStateException} will be thrown.
     */
    @Override
    void unlock();

    /**
     * Schedules unlock operation for asynchronous processing but doesn't await for its completion. If current thread is not the lock
     * owner then {@link IllegalMonitorStateException} will be thrown.
     */
    void unlockAsync();

    /**
     * Tries to acquire the lock with the given timeout.
     *
     * <p>
     * <b>Note:</b> timeout doesn't consider communication overhead of locking. For example, if timeout is 100ms and it takes 50ms to
     * communicate with the cluster members in order to try the lock then the total wait time before giving up will be 150ms.
     * </p>
     *
     * @param timeout Maximum time to wait for the lock (doesn't include the communication overhead).
     * @param unit Time unit of timeout.
     *
     * @return {@code true} if the lock was acquired and {@code false} if the waiting time elapsed before the lock was acquired.
     *
     * @throws InterruptedException Signals that current thread is interrupted while acquiring the lock.
     */
    @Override
    boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Returns an information about the node that is currently holding this lock.
     *
     * <p>
     * Note that this operation requires a network round trip to the lock manager node and there are no guarantees that lock owner will not
     * change by the time when result is returned from this method.
     * </p>
     *
     * @return Lock owner.
     *
     * @throws InterruptedException Signal that current thread was interrupted while awaiting for lock owner information.
     */
    Optional<LockOwnerInfo> owner() throws InterruptedException;

    /**
     * Unsupported operation.
     *
     * @return Unsupported operation.
     */
    @Override
    Condition newCondition();
}
