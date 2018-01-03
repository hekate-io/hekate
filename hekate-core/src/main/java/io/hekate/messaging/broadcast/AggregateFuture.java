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

package io.hekate.messaging.broadcast;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.MessagingFutureException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Asynchronous result of {@link MessagingChannel#aggregate(Object) aggregate(...)} operation.
 *
 * @param <T> Base type of aggregation results.
 *
 * @see MessagingChannel#aggregate(Object)
 * @see AggregateResult
 */
public class AggregateFuture<T> extends MessagingFuture<AggregateResult<T>> {
    /**
     * Awaits for the asynchronous aggregation to complete and returns the {@link AggregateResult#results()} result}.
     *
     * @return Aggregation result.
     *
     * @throws MessagingFutureException Signals that aggregation failed.
     * @throws InterruptedException Signals that the thread was interrupted while waiting for aggregation to complete.
     */
    public Collection<T> results() throws InterruptedException, MessagingFutureException {
        return get().results();
    }

    /**
     * Awaits for the asynchronous aggregation to complete within the timeout and returns the {@link AggregateResult#results()} result}.
     *
     * @param timeout Time to wait for operation result.
     * @param unit Time unit of the timeout argument
     *
     * @return Aggregation result.
     *
     * @throws MessagingFutureException Signals that aggregation failed.
     * @throws InterruptedException Signals that the thread was interrupted while waiting for aggregation to complete.
     * @throws TimeoutException Signals that timeout happened.
     */
    public Collection<T> results(long timeout, TimeUnit unit) throws InterruptedException, MessagingFutureException, TimeoutException {
        return get(timeout, unit).results();
    }

    /**
     * Uninterruptedly awaits for the asynchronous aggregation to complete and returns the {@link AggregateResult#results()} result}.
     *
     * @return Aggregation result.
     *
     * @throws MessagingFutureException Signals that aggregation failed.
     */
    public Collection<T> resultsUninterruptedly() throws MessagingFutureException {
        return getUninterruptedly().results();
    }

    /**
     * Awaits for the asynchronous aggregation to complete and performs the given action for each element of the aggregation {@link
     * #results() results}.
     *
     * @param action The action to be performed for each result.
     *
     * @throws MessagingFutureException Signals that aggregation failed.
     * @throws InterruptedException Signals that the thread was interrupted while waiting for aggregation to complete.
     */
    public void forEach(Consumer<? super T> action) throws InterruptedException, MessagingFutureException {
        results().forEach(action);
    }
}
