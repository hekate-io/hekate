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

package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.Reply;
import io.hekate.messaging.unicast.RequestCallback;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class RequestCallbackMock implements RequestCallback<String> {
    private final String expectedRequest;

    private final CompletableFuture<Reply<String>> latch = new CompletableFuture<>();

    public RequestCallbackMock(String expectedRequest) {
        this.expectedRequest = expectedRequest;
    }

    public Reply<String> get() throws Exception {
        try {
            return latch.get(3, TimeUnit.SECONDS);
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

    @Override
    public void onComplete(Throwable err, Reply<String> reply) {
        if (err == null) {
            try {
                assertEquals(expectedRequest, reply.getRequest());

                latch.complete(reply);
            } catch (AssertionError e) {
                latch.completeExceptionally(e);
            }
        } else {
            try {
                latch.completeExceptionally(err);
            } catch (AssertionError e) {
                latch.completeExceptionally(e);
            }
        }
    }

    protected void fail(Throwable error) {
        latch.completeExceptionally(error);
    }
}
