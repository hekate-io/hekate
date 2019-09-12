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

package io.hekate.messaging.internal;

import io.hekate.messaging.operation.SendCallback;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SendCallbackMock implements SendCallback {
    private final CompletableFuture<Void> latch = new CompletableFuture<>();

    @Override
    public final void onComplete(Throwable err) {
        if (err == null) {
            try {
                onSendSuccess();

                latch.complete(null);
            } catch (RuntimeException | Error e) {
                latch.completeExceptionally(e);
            }
        } else {
            latch.completeExceptionally(err);
        }
    }

    public Void get() throws Exception {
        try {
            return latch.get();
        } catch (ExecutionException e) {
            Throwable error = e.getCause();

            if (error instanceof Exception) {
                throw (Exception)error;
            } else if (error instanceof Error) {
                throw (Error)error;
            } else {
                throw new Exception(error);
            }
        }
    }

    protected void onSendSuccess() {
        // No-op.
    }
}
