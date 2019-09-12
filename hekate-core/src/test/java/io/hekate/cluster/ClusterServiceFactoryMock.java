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

package io.hekate.cluster;

import io.hekate.cluster.internal.gossip.GossipListener;
import io.hekate.cluster.internal.gossip.GossipNodeStatus;
import io.hekate.cluster.internal.gossip.GossipProtocol;
import io.hekate.util.StateGuard;
import java.util.Optional;
import java.util.Set;

public class ClusterServiceFactoryMock extends ClusterServiceFactory {
    public static class GossipSpy implements GossipListener {
        private volatile GossipListener delegate;

        private volatile boolean hasNodeFailures;

        public void setDelegate(GossipListener delegate) {
            this.delegate = delegate;
        }

        public boolean isHasNodeFailures() {
            return hasNodeFailures;
        }

        @Override
        public void onJoinReject(ClusterAddress rejectedBy, String reason) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onJoinReject(rejectedBy, reason);
            }
        }

        @Override
        public void onStatusChange(GossipNodeStatus oldStatus, GossipNodeStatus newStatus, int order, Set<ClusterNode> topology) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onStatusChange(oldStatus, newStatus, order, topology);
            }
        }

        @Override
        public void onTopologyChange(Set<ClusterNode> oldTopology, Set<ClusterNode> newTopology, Set<ClusterNode> failed) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onTopologyChange(oldTopology, newTopology, failed);
            }
        }

        @Override
        public void onKnownAddressesChange(Set<ClusterAddress> oldAddresses, Set<ClusterAddress> newAddresses) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onKnownAddressesChange(oldAddresses, newAddresses);
            }
        }

        @Override
        public void onNodeFailureSuspected(ClusterNode failed, GossipNodeStatus status) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onNodeFailureSuspected(failed, status);
            }
        }

        @Override
        public void onNodeFailureUnsuspected(ClusterNode node, GossipNodeStatus status) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onNodeFailureUnsuspected(node, status);
            }
        }

        @Override
        public void onNodeFailure(ClusterNode failed, GossipNodeStatus status) {
            GossipListener delegate = this.delegate;

            hasNodeFailures = true;

            if (delegate != null) {
                delegate.onNodeFailure(failed, status);
            }
        }

        @Override
        public void onNodeInconsistency(GossipNodeStatus status) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                delegate.onNodeInconsistency(status);
            }
        }

        @Override
        public Optional<Throwable> onBeforeSend(GossipProtocol msg) {
            GossipListener delegate = this.delegate;

            if (delegate != null) {
                return delegate.onBeforeSend(msg);
            }

            return Optional.empty();
        }
    }

    public ClusterServiceFactoryMock() {
        setGossipSpy(new GossipSpy());
        setServiceGuard(new StateGuard(ClusterService.class));
    }

    @Override
    public StateGuard getServiceGuard() {
        return super.getServiceGuard();
    }

    @Override
    public void setServiceGuard(StateGuard serviceGuard) {
        super.setServiceGuard(serviceGuard);
    }

    @Override
    public GossipSpy getGossipSpy() {
        return (GossipSpy)super.getGossipSpy();
    }

    @Override
    public void setGossipSpy(GossipListener gossipSpy) {
        super.setGossipSpy(gossipSpy);
    }
}
