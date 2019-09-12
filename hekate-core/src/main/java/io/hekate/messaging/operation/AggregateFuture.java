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
import java.util.Collection;

/**
 * Asynchronous result of an {@link Aggregate} operation.
 *
 * @param <T> Base type of aggregation results.
 *
 * @see MessagingChannel#newAggregate(Object)
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
     * Awaits for the asynchronous aggregation to complete and returns the {@link AggregateResult#results()} result}.
     *
     * @param type Result type.
     * @param <V> Result type.
     *
     * @return Aggregation result.
     *
     * @throws MessagingFutureException Signals that aggregation failed.
     * @throws InterruptedException Signals that the thread was interrupted while waiting for aggregation to complete.
     */
    public <V extends T> Collection<V> results(Class<V> type) throws InterruptedException, MessagingFutureException {
        Collection<T> results = get().results();

        return cast(type, results);
    }

    @SuppressWarnings("unchecked")
    private <V extends T> Collection<V> cast(Class<V> type, Collection<T> results) {
        assert results.stream().allMatch(type::isInstance) : "Not all of the aggregation results can be cast to " + type.getName();

        // No real type checking for performance reasons.
        return (Collection<V>)results;
    }
}
