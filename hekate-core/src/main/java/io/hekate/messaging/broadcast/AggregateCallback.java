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

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.unicast.Response;

/**
 * Callback for {@link MessagingChannel#aggregate(Object, AggregateCallback) aggregate(...)} operation.
 *
 * <p>
 * This interface can be used with the {@link MessagingChannel#aggregate(Object, AggregateCallback)} method and will be notified as
 * follows:
 * </p>
 * <ul>
 * <li>channel submits request to each of the target nodes in parallel</li>
 * <li>for each failed submission the {@link #onReplyFailure(Object, ClusterNode, Throwable)} method gets notified</li>
 * <li>for each successfully received response the {@link #onReplySuccess(Response, ClusterNode)} method gets notified</li>
 * <li>for each response receiving failure the {@link #onReplyFailure(Object, ClusterNode, Throwable)} method gets notified</li>
 * <li>when all results are collected (either successful or failed) the {@link #onComplete(Throwable, AggregateResult)} method gets
 * called with an error parameter being {@code null} and an {@link AggregateResult} object containing all successful and partially failed
 * results.</li>
 * </ul>
 *
 * <p>
 * <b>Note:</b> Implementations of this interface must be thread safe since its methods can be notified in parallel by multiple threads.
 * </p>
 *
 * @param <T> Base type of a messages.
 *
 * @see MessagingChannel#aggregate(Object, AggregateCallback)
 * @see AggregateResult
 */
@FunctionalInterface
public interface AggregateCallback<T> {
    /**
     * Called when the aggregation operation gets completed either successfully or with an error.
     *
     * <p>
     * If operation completes with a fatal error (f.e channel is closed) then {@code err} parameter will hold the error cause and
     * {@code result} parameter will be {@code null}. Otherwise {@code err} parameter will be {@code null} and {@code result} parameter
     * will hold the aggregation result.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param result Aggregation result ({@code null} if operation failed).
     */
    void onComplete(Throwable err, AggregateResult<T> result);

    /**
     * Called after a reply was received from a cluster node.
     *
     * <p>
     * Note that this method gets called while aggregation is still in progress. Final aggregation results are provided by the {@link
     * #onComplete(Throwable, AggregateResult)} method. See the description of {@link AggregateCallback} interface for the complete
     * sequence of callback methods notification.
     * </p>
     *
     * @param rsp Response from the cluster node.
     * @param node Cluster node that sent the reply.
     */
    default void onReplySuccess(Response<T> rsp, ClusterNode node) {
        // No-op.
    }

    /**
     * Called in case of a cluster node communication failure.
     *
     * <p>
     * Note that this method gets called while aggregation is still in progress. Final aggregation results are provided by the {@link
     * #onComplete(Throwable, AggregateResult)} method. See the description of {@link AggregateCallback} interface for the complete
     * sequence of callback methods notification.
     * </p>
     *
     * @param request Original request.
     * @param node Failed cluster node.
     * @param cause Error cause.
     */
    default void onReplyFailure(T request, ClusterNode node, Throwable cause) {
        // No-op.
    }
}
