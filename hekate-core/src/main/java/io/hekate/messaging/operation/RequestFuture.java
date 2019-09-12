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

package io.hekate.messaging.operation;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.MessagingFutureException;

/**
 * Asynchronous result of {@link Request} operation.
 *
 * @param <T> Base type of request message.
 *
 * @see MessagingChannel#newRequest(Object)
 */
public class RequestFuture<T> extends MessagingFuture<Response<T>> {
    /**
     * Awaits for this operation to complete and returns the {@link Response#payload() payload} of the response.
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     * @throws InterruptedException Signals that thread was interrupted while awaiting for operation completion.
     */
    public T result() throws MessagingFutureException, InterruptedException {
        return get().payload();
    }

    /**
     * Awaits for this operation to complete and returns the {@link Response#payload() payload} of the response.
     *
     * @param type Response type.
     * @param <V> Response type.
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     * @throws InterruptedException Signals that thread was interrupted while awaiting for this operation to complete.
     */
    public <V extends T> V result(Class<V> type) throws MessagingFutureException, InterruptedException {
        return type.cast(get().payload());
    }
}
