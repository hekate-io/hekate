/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.messaging.unicast;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.MessagingFutureException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Asynchronous result of {@link MessagingChannel#request(Object) request(...)} operation.
 *
 * @param <T> Base type of request message.
 *
 * @see MessagingChannel#request(Object)
 */
public class ResponseFuture<T> extends MessagingFuture<Response<T>> {
    /**
     * Awaits for the asynchronous operation to complete and returns the {@link Response#get() payload} of reply.
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     * @throws InterruptedException Signals that thread was interrupted while awaiting for operation completion.
     */
    public T response() throws MessagingFutureException, InterruptedException {
        return get().get();
    }

    /**
     * Awaits for the asynchronous operation to complete within the timeout and returns the {@link Response#get() payload} of reply. Throws
     * {@link TimeoutException} if timeout happens before operation results are ready.
     *
     * @param timeout Time to wait for operation result.
     * @param unit Time unit of the timeout argument
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     * @throws TimeoutException Signals that timeout happened.
     * @throws InterruptedException Signals that thread was interrupted while awaiting for operation completion.
     */
    public T response(long timeout, TimeUnit unit) throws MessagingFutureException, InterruptedException, TimeoutException {
        return get(timeout, unit).get();
    }

    /**
     * Awaits for the asynchronous operation to complete within the timeout and returns the {@link Response#get() payload} of reply. Throws
     * {@link TimeoutException} if timeout happens before operation results are ready.
     *
     * @param type Response type.
     * @param timeout Time to wait for operation result.
     * @param unit Time unit of the timeout argument
     * @param <V> Response type.
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     * @throws TimeoutException Signals that timeout happened.
     * @throws InterruptedException Signals that thread was interrupted while awaiting for operation completion.
     */
    public <V extends T> V response(Class<V> type, long timeout, TimeUnit unit) throws MessagingFutureException, InterruptedException,
        TimeoutException {
        return type.cast(get(timeout, unit).get());
    }

    /**
     * Awaits for the asynchronous operation to complete and returns the {@link Response#get() payload} of reply.
     *
     * @param type Response type.
     * @param <V> Response type.
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     * @throws InterruptedException Signals that thread was interrupted while awaiting for operation completion.
     */
    public <V extends T> V response(Class<V> type) throws MessagingFutureException, InterruptedException {
        return type.cast(get().get());
    }

    /**
     * Uninterruptedly awaits for the asynchronous operation to complete and returns the {@link Response#get() payload} of reply.
     *
     * <p>
     * <b>Note:</b> if thread gets interrupted while awaiting for operation to complete then such interruption will be ignored and thread's
     * {@link Thread#isInterrupted() interrupted} flag will be set to {@code true} after operation completion.
     * </p>
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     */
    public T responseUninterruptedly() throws MessagingFutureException {
        return getUninterruptedly().get();
    }

    /**
     * Uninterruptedly awaits for the asynchronous operation to complete and returns the {@link Response#get() payload} of reply.
     *
     * <p>
     * <b>Note:</b> if thread gets interrupted while awaiting for operation to complete then such interruption will be ignored and thread's
     * {@link Thread#isInterrupted() interrupted} flag will be set to {@code true} after operation completion.
     * </p>
     *
     * @param type Response type.
     * @param <V> Response type.
     *
     * @return Response.
     *
     * @throws MessagingFutureException Signals that request operation failed.
     */
    public <V extends T> V responseUninterruptedly(Class<V> type) throws MessagingFutureException {
        return type.cast(getUninterruptedly().get());
    }
}
