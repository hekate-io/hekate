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

import java.util.concurrent.CompletableFuture;

class LockFuture extends CompletableFuture<Boolean> {
    private final LockControllerClient handle;

    public LockFuture(LockControllerClient handle) {
        this.handle = handle;
    }

    public static LockFuture completed(boolean result) {
        LockFuture future = new LockFuture(null);

        future.complete(result);

        return future;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (super.cancel(mayInterruptIfRunning)) {
            if (handle != null) {
                handle.becomeUnlockingIfNotLocked();
            }

            return true;
        }

        return false;
    }
}
