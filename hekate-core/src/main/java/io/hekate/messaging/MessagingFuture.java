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

package io.hekate.messaging;

import io.hekate.util.HekateFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Asynchronous result of a messaging operation.
 *
 * @param <T> Type of operation result.
 */
public class MessagingFuture<T> extends HekateFuture<T, MessagingFuture<T>> {
    @Override
    public T get() throws InterruptedException, MessagingFutureException {
        try {
            return super.get();
        } catch (ExecutionException e) {
            throw new MessagingFutureException("Messaging operation failed (see cause for details).", e.getCause());
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, MessagingFutureException, TimeoutException {
        try {
            return super.get(timeout, unit);
        } catch (ExecutionException e) {
            throw new MessagingFutureException("Messaging operation failed (see cause for details).", e.getCause());
        }
    }

    @Override
    protected MessagingFuture<T> newInstance() {
        return new MessagingFuture<>();
    }
}
