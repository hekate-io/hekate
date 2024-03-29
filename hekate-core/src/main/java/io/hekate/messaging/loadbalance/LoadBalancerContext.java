/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.messaging.loadbalance;

import io.hekate.cluster.ClusterTopology;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;
import java.util.function.Function;

/**
 * Context for {@link LoadBalancer}.
 *
 * @see LoadBalancer
 */
public interface LoadBalancerContext extends ClusterTopology {
    /**
     * Returns the cluster topology.
     *
     * @return Cluster topology.
     */
    @Override
    ClusterTopology topology();

    /**
     * Returns the partition mapper.
     *
     * @return Mapper.
     */
    PartitionMapper partitions();

    /**
     * Returns {@code true} if the messaging operation has an affinity key (see {@link #affinityKey()}).
     *
     * @return {@code true} if the messaging operation has an affinity key.
     */
    boolean hasAffinity();

    /**
     * Returns the hash code of affinity key or a synthetically generated value if affinity key was not specified for the messaging
     * operation.
     *
     * @return Hash code of affinity key.
     */
    int affinity();

    /**
     * Returns the affinity key of the messaging operation or {@code null} if the affinity key wasn't specified.
     *
     * @return Affinity key or {@code null}.
     */
    Object affinityKey();

    /**
     * Returns information about a failure if this context represents a retry attempts.
     *
     * @return Information about a retry attempt.
     *
     * @see RetryErrorPredicate
     */
    Optional<FailedAttempt> failure();

    /**
     * Constructs a new context object or returns a cached one, based on the current cluster topology.
     *
     * <p>
     * This method provides support to cache user context objects that are necessary for message routing and are expensive to construct
     * on each routing operation. Such objects are cached at the {@link MessagingChannel} instance level until the topology changes.
     * When topology changes, a new object is lazily constructed via the provided {@code supplier} function.
     * </p>
     *
     * <p>
     * Note that only one context object is supported per {@link MessagingChannel} instance. Any attempt to use different {@code supplier}
     * functions will lead to unpredictable results.
     * </p>
     *
     * @param supplier Context supplier (must be idempotent and free of side effects).
     * @param <T> Context type.
     *
     * @return Context object.
     */
    <T> T topologyContext(Function<ClusterTopology, T> supplier);
}
