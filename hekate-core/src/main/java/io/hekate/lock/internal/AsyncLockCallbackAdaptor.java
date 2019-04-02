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

import io.hekate.lock.AsyncLockCallback;
import io.hekate.lock.LockOwnerInfo;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AsyncLockCallbackAdaptor {
    private static final Logger log = LoggerFactory.getLogger(AsyncLockCallbackAdaptor.class);

    private final DefaultDistributedLock lock;

    private final Executor executor;

    private final AsyncLockCallback callback;

    public AsyncLockCallbackAdaptor(DefaultDistributedLock lock, Executor executor, AsyncLockCallback callback) {
        this.lock = lock;
        this.executor = executor;
        this.callback = callback;
    }

    public void onLockAcquire(LockControllerClient handleInfo) {
        try {
            executor.execute(() -> {
                try {
                    lock.registerLock(handleInfo);

                    callback.onLockAcquire(lock);
                } catch (RuntimeException | Error e) {
                    log.error("Failed to notify callback on lock acquisition.", e);
                }
            });
        } catch (RuntimeException | Error e) {
            log.error("Failed to schedule asynchronous notification of lock acquisition callback.", e);
        }
    }

    public void onLockBusy(LockOwnerInfo owner) {
        try {
            executor.execute(() -> {
                try {
                    callback.onLockBusy(owner);
                } catch (RuntimeException | Error e) {
                    log.error("Failed to notify callback on lock busy.", e);
                }
            });
        } catch (RuntimeException | Error e) {
            log.error("Failed to schedule asynchronous notification of busy lock callback.", e);
        }
    }

    public void onLockOwnerChange(LockOwnerInfo owner) {
        try {
            executor.execute(() -> {
                try {
                    callback.onLockOwnerChange(owner);
                } catch (RuntimeException | Error e) {
                    log.error("Failed to notify callback on lock owner change.", e);
                }
            });
        } catch (RuntimeException | Error e) {
            log.error("Failed to schedule asynchronous notification of lock owner change callback.", e);
        }
    }

    public void onLockRelease() {
        try {
            executor.execute(() -> {
                try {
                    lock.clearLock();

                    callback.onLockRelease(lock);
                } catch (RuntimeException | Error e) {
                    log.error("Failed to notify callback on lock release.", e);
                }
            });
        } catch (RuntimeException | Error e) {
            log.error("Failed to schedule asynchronous notification of lock release callback.", e);
        }
    }
}
