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

package io.hekate.messaging.loadbalance;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.HekateSupport;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.MessagingChannel;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

/**
 * Context for {@link LoadBalancer}.
 *
 * @see LoadBalancer
 */
public interface LoadBalancerContext extends ClusterTopology, HekateSupport {
    /**
     * Returns the cluster topology.
     *
     * @return Cluster topology.
     */
    @Override
    ClusterTopology topology();

    /**
     * Returns the default partition mapper that the channel uses to map {@link MessagingChannel#withAffinity(Object) affinity keys} to the
     * cluster nodes.
     *
     * @return Mapper.
     */
    PartitionMapper partitions();

    /**
     * Returns <tt>true</tt> if the messaging operation has an affinity key (see {@link #affinityKey()}).
     *
     * @return <tt>true</tt> if the messaging operation has an affinity key.
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
     *
     * @see MessagingChannel#withAffinity(Object)
     */
    Object affinityKey();

    /**
     * Returns information about a failover attempt if this context represents a {@link FailoverPolicy} re-routing attempts.
     *
     * @return Information about a failover attempt.
     *
     * @see FailoverPolicy
     */
    Optional<FailureInfo> failure();

    @Override
    LoadBalancerContext filter(ClusterNodeFilter filter);

    @Override
    LoadBalancerContext filterAll(ClusterFilter filter);
}
