/*
 * Copyright 2020 The Hekate Project
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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.lock.AsyncLockCallback;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultDistributedLock implements DistributedLock {
    private static class LocalLock {
        private final long id;

        private int hold = 1;

        public LocalLock(long id) {
            this.id = id;
        }

        public long id() {
            return id;
        }

        void increment() {
            hold++;
        }

        boolean decrement() {
            hold--;

            return hold == 0;
        }

        int holdCount() {
            return hold;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultDistributedLock.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final ThreadLocal<Map<DefaultLockRegion, Map<String, LocalLock>>> THREAD_LOCAL_LOCKS = new ThreadLocal<>();

    private final String name;

    @ToStringIgnore
    private final DefaultLockRegion region;

    public DefaultDistributedLock(String name, DefaultLockRegion region) {
        assert name != null : "Name is null.";
        assert region != null : "Lock region is null.";

        this.name = name;
        this.region = region;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String regionName() {
        return region.name();
    }

    @Override
    public int holdCount() {
        LocalLock localLock = existingLock();

        return localLock == null ? 0 : localLock.holdCount();
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return holdCount() > 0;
    }

    @Override
    public Future<?> lockAsync(Executor executor, AsyncLockCallback callback) {
        ArgAssert.notNull(executor, "Executor");
        ArgAssert.notNull(callback, "Callback");

        if (DEBUG) {
            log.debug("Locking asynchronously [lock={}]", this);
        }

        AsyncLockCallbackAdaptor adaptor = new AsyncLockCallbackAdaptor(this, executor, callback);

        LockControllerClient handle = region.lock(DefaultLockRegion.TIMEOUT_UNBOUND, this, adaptor);

        return handle.lockFuture();
    }

    @Override
    public void lock() {
        LocalLock localLock = existingLock();

        if (localLock == null) {
            if (DEBUG) {
                log.debug("Locking [lock={}]", this);
            }

            LockControllerClient lock = region.lock(DefaultLockRegion.TIMEOUT_UNBOUND, this);

            try {
                AsyncUtils.getUninterruptedly(lock.lockFuture());

                registerLock(lock.lockId());

                if (DEBUG) {
                    log.debug("Locked [lock={}]", this);
                }
            } catch (ExecutionException e) {
                throw convertError(e);
            }
        } else {
            localLock.increment();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        LocalLock localLock = existingLock();

        if (localLock == null) {
            if (DEBUG) {
                log.debug("Locking [lock={}]", this);
            }

            LockControllerClient lock = region.lock(DefaultLockRegion.TIMEOUT_UNBOUND, this);

            try {
                lock.lockFuture().get();

                registerLock(lock.lockId());

                if (DEBUG) {
                    log.debug("Locked [lock={}]", this);
                }
            } catch (InterruptedException e) {
                // Do not wait for lock to be released.
                region.unlock(lock.lockId());

                throw e;
            } catch (ExecutionException e) {
                throw convertError(e);
            }
        } else {
            localLock.increment();
        }
    }

    @Override
    public boolean tryLock() {
        LocalLock localLock = existingLock();

        if (localLock == null) {
            if (DEBUG) {
                log.debug("Trying lock [lock={}]", this);
            }

            LockControllerClient lock = region.lock(DefaultLockRegion.TIMEOUT_IMMEDIATE, this);

            try {
                boolean locked = AsyncUtils.getUninterruptedly(lock.lockFuture());

                if (locked) {
                    registerLock(lock.lockId());
                }

                if (DEBUG) {
                    if (locked) {
                        log.debug("Locked [lock={}]", this);
                    } else {
                        log.debug("Lock is busy [lock={}]", this);
                    }
                }

                return locked;
            } catch (ExecutionException e) {
                throw convertError(e);
            }
        } else {
            localLock.increment();

            return true;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long timeNanos = unit.toNanos(time);

        if (timeNanos <= 0) {
            return tryLock();
        } else {
            LocalLock localLock = existingLock();

            if (localLock == null) {
                if (DEBUG) {
                    log.debug("Trying lock [timeout={}, unit={}, lock={}]", time, unit, this);
                }

                LockControllerClient lock = region.lock(timeNanos, this);

                try {
                    boolean locked = lock.lockFuture().get();

                    if (locked) {
                        registerLock(lock.lockId());
                    }

                    if (DEBUG) {
                        if (locked) {
                            log.debug("Locked [lock={}]", this);
                        } else {
                            log.debug("Lock timeout [lock={}]", this);
                        }
                    }

                    return locked;
                } catch (InterruptedException e) {
                    // Do not wait for lock to be released.
                    region.unlock(lock.lockId());

                    throw e;
                } catch (ExecutionException e) {
                    throw convertError(e);
                }
            } else {
                localLock.increment();

                return true;
            }
        }
    }

    @Override
    public Optional<LockOwnerInfo> owner() throws InterruptedException {
        return region.ownerOf(name);
    }

    @Override
    public void unlock() {
        doUnlock(true);
    }

    @Override
    public void unlockAsync() {
        doUnlock(false);
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    private void doUnlock(boolean sync) {
        LocalLock lock = existingLock();

        if (lock != null) {
            if (lock.decrement()) {
                if (DEBUG) {
                    log.debug("Unlocking [lock={}]", this);
                }

                clearLock();

                LockFuture future = region.unlock(lock.id());

                if (sync) {
                    try {
                        AsyncUtils.getUninterruptedly(future);

                        if (DEBUG) {
                            log.debug("Unlocked [lock={}]", this);
                        }
                    } catch (ExecutionException e) {
                        throw convertError(e);
                    }
                }
            }
        } else {
            throw new IllegalMonitorStateException("Lock is not held by the current thread.");
        }
    }

    void registerLock(long id) {
        LocalLock lock = new LocalLock(id);

        Map<DefaultLockRegion, Map<String, LocalLock>> managers = THREAD_LOCAL_LOCKS.get();

        if (managers == null) {
            managers = new IdentityHashMap<>();

            THREAD_LOCAL_LOCKS.set(managers);
        }

        Map<String, LocalLock> locks = managers.computeIfAbsent(region, k -> new HashMap<>());

        locks.put(name, lock);
    }

    boolean clearLock() {
        boolean removed = false;

        Map<DefaultLockRegion, Map<String, LocalLock>> managers = THREAD_LOCAL_LOCKS.get();

        if (managers != null) {
            Map<String, LocalLock> locks = managers.get(region);

            if (locks != null) {
                removed = locks.remove(name) != null;

                if (removed) {
                    if (locks.isEmpty()) {
                        managers.remove(region);
                    }

                    if (managers.isEmpty()) {
                        THREAD_LOCAL_LOCKS.set(null);
                    }
                }
            }
        }

        return removed;
    }

    private LocalLock existingLock() {
        Map<DefaultLockRegion, Map<String, LocalLock>> managers = THREAD_LOCAL_LOCKS.get();

        if (managers != null) {
            Map<String, LocalLock> locks = managers.get(region);

            if (locks != null) {
                return locks.get(name);
            }
        }

        return null;
    }

    private RuntimeException convertError(ExecutionException e) {
        if (e.getCause() instanceof RuntimeException) {
            throw (RuntimeException)e.getCause();
        } else if (e.getCause() instanceof Error) {
            throw (Error)e.getCause();
        } else {
            // Should never happen.
            throw new AssertionError("Unexpected checked exception: " + e.toString(), e);
        }
    }

    @Override
    public String toString() {
        return ToString.format(DistributedLock.class, this);
    }
}
