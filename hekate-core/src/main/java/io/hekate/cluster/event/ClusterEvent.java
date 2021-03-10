/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.cluster.event;

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.HekateSupport;
import java.util.concurrent.CompletableFuture;

/**
 * Cluster event. This is the base interface for all events that can be fired by the {@link ClusterService}.
 *
 * @see ClusterEventListener
 * @see ClusterService
 */
public interface ClusterEvent extends HekateSupport {
    /**
     * Returns the event type.
     *
     * @return Event type.
     */
    ClusterEventType type();

    /**
     * Casts this instance to the {@link ClusterJoinEvent} type or returns {@code null} if this instance can't be cast to that type.
     *
     * @return This instance as to {@link ClusterJoinEvent} or {@code null} if this instance can't be cast to that type.
     */
    ClusterJoinEvent asJoin();

    /**
     * Casts this instance to the {@link ClusterLeaveEvent} type or returns {@code null} if this instance can't be cast to that type.
     *
     * @return This instance as to {@link ClusterLeaveEvent} or {@code null} if this instance can't be cast to that type.
     */
    ClusterLeaveEvent asLeave();

    /**
     * Casts this instance to the {@link ClusterChangeEvent} type or returns {@code null} if this instance can't be cast to that type.
     *
     * @return This instance as to {@link ClusterChangeEvent} or {@code null} if this instance can't be cast to that type.
     */
    ClusterChangeEvent asChange();

    /**
     * Returns the cluster topology snapshot of this event.
     *
     * @return Cluster topology.
     */
    ClusterTopology topology();

    /**
     * Attaches an arbitrary asynchronous task to this event.
     *
     * <p>
     * The specified future object represents an asynchronous task that should be logically attached to this event in such a way that this
     * event should not be considered completed until the task future is completed.
     * </p>
     *
     * @param future Synchronization future.
     *
     * @see #future()
     */
    void attach(CompletableFuture<?> future);

    /**
     * Returns a future object that represents all the {@link #attach(CompletableFuture) attached} futures of this event.
     *
     * @return Future object that represents all the {@link #attach(CompletableFuture) attached} futures of this event.
     *
     * @see #attach(CompletableFuture)
     */
    CompletableFuture<?> future();
}
