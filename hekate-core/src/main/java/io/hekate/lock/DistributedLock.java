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
 * Distributed locks provide support for controlling access to a shared resource or a critical section at the cluster-wide level. At any
 * point in time only one thread on any cluster node can gain a lock with the same name. All other threads either running on the same node
 * or on remote nodes will await for the lock to be released before getting a chance to obtain the lock.
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
     * Performs asynchronous locking operation and notifies the specified callback upon lock status changes.
     *
     * <p>
     * The specified {@link Executor} instance will be used to perform all callback method notifications. Thus it is <b>highly recommended
     * to use {@link Executors#newSingleThreadExecutor() single-threaded executor}</b> so that all callback notifications would be
     * performed by the same thread. Otherwise callback methods notification order can be completely unpredictable.
     * </p>
     *
     * <p>
     * Once lock gets acquired it will be bound to the executor thread that performed
     * {@link AsyncLockCallback#onLockAcquire(DistributedLock)} notification. For more details about callback methods notification order
     * please see the documentation of {@link AsyncLockCallback} interface.
     * </p>
     *
     * <p>
     * The returned {@link Future} object can be used to wait for asynchronous lock acquisition (after lock have been acquired but before
     * {@link AsyncLockCallback#onLockAcquire(DistributedLock)} is called) or cancel locking operation if lock haven't been acquired yet.
     * </p>
     *
     * @param executor Executor to perform asynchronous notifications of callback methods.
     * @param callback Callback.
     *
     * @return Future object that can be used to await for lock to be asynchronously acquired. Future gets completed after lock have been
     * acquired but before {@link AsyncLockCallback#onLockAcquire(DistributedLock)} is called.
     */
    Future<?> lockAsync(Executor executor, AsyncLockCallback callback);

    /**
     * Schedules unlock operation for asynchronous processing and uninterruptedly awaits for completion. If current thread is not the lock
     * owner then {@link IllegalMonitorStateException} will be thrown.
     */
    @Override
    void unlock();

    /**
     * Schedules unlock operation for asynchronous processing but doesn't await for its completion. If current thread is not the lock
     * owner then {@link IllegalMonitorStateException} will be thrown.
     */
    void unlockAsync();

    /**
     * Acquires the lock if it is free within the given waiting time and the current thread has not been {@linkplain Thread#interrupt
     * interrupted}.
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
     * Returns information about the node that is currently holding this lock.
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
