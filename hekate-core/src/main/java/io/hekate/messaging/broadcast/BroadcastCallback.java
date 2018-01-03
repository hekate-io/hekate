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

/**
 * Callback for {@link MessagingChannel#broadcast(Object, BroadcastCallback) broadcast(...)} operation.
 *
 * <p>
 * This interface can be used with the {@link MessagingChannel#broadcast(Object, BroadcastCallback)} method and will be notified as
 * follows:
 * </p>
 * <ul>
 * <li>channel submits message to each of the target nodes in parallel</li>
 * <li>for each successful submission the {@link #onSendSuccess(Object, ClusterNode)} method gets notified</li>
 * <li>for each failed submission the {@link #onSendFailure(Object, ClusterNode, Throwable)} method gets notified</li>
 * <li>when message gets submitted to all nodes (either successfully or with a failure) then {@link #onComplete(Throwable,
 * BroadcastResult)} method gets called with an error parameter being {@code null} and {@link BroadcastResult} containing information
 * about
 * partial failures.</li>
 * </ul>
 *
 * <p>
 * <b>Note:</b> Implementations of this interface must be thread safe since its methods can be notified in parallel by multiple threads.
 * </p>
 *
 * @param <T> Base type of a messages.
 *
 * @see MessagingChannel#broadcast(Object, BroadcastCallback)
 * @see BroadcastResult
 */
@FunctionalInterface
public interface BroadcastCallback<T> {
    /**
     * Called when the broadcast operation gets completed either successfully or with an error.
     *
     * <p>
     * If operation completes with a fatal error (f.e channel is closed) then {@code err} parameter will hold the error cause and
     * {@code result} parameter will be {@code null}. Otherwise {@code err} parameter will be {@code null} and {@code result} parameter
     * will hold the broadcast operation result.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param result Broadcast operation result ({@code null} if operation failed).
     */
    void onComplete(Throwable err, BroadcastResult<T> result);

    /**
     * Called when message was successfully submitted to the cluster node.
     *
     * <p>
     * Note that this method gets called while broadcast is still in progress. Final broadcast results are provided by the {@link
     * #onComplete(Throwable, BroadcastResult)} method. See the description of {@link BroadcastCallback} interface for the complete
     * sequence of callback methods notification.
     * </p>
     *
     * @param message Message.
     * @param node Cluster node.
     */
    default void onSendSuccess(T message, ClusterNode node) {
        // No-op.
    }

    /**
     * Called in case of a cluster node communication failure.
     *
     * <p>
     * Note that this method gets called while broadcast is still in progress. Final broadcast results are provided by the {@link
     * #onComplete(Throwable, BroadcastResult)} method. See the description of {@link BroadcastCallback} interface for the complete
     * sequence of callback methods notification.
     * </p>
     *
     * @param message Message.
     * @param node Failed cluster node.
     * @param error Error cause.
     */
    default void onSendFailure(T message, ClusterNode node, Throwable error) {
        // No-op.
    }
}
