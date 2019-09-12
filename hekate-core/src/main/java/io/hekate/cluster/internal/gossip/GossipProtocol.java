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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;

public abstract class GossipProtocol {
    public enum Type {
        LONG_TERM_CONNECT,

        GOSSIP_UPDATE,

        GOSSIP_UPDATE_DIGEST,

        JOIN_REQUEST,

        JOIN_ACCEPT,

        JOIN_REJECT,

        HEARTBEAT_REQUEST,

        HEARTBEAT_REPLY
    }

    public abstract static class GossipMessage extends GossipProtocol {
        private final ClusterAddress to;

        public GossipMessage(ClusterAddress from, ClusterAddress to) {
            super(from);

            this.to = to;
        }

        public ClusterAddress to() {
            return to;
        }

        @Override
        public InetSocketAddress toAddress() {
            return to.socket();
        }
    }

    public static class LongTermConnect extends GossipMessage {
        public LongTermConnect(ClusterAddress from, ClusterAddress to) {
            super(from, to);
        }

        @Override
        public Type type() {
            return Type.LONG_TERM_CONNECT;
        }
    }

    public static class HeartbeatReply extends GossipMessage {
        public HeartbeatReply(ClusterAddress from, ClusterAddress to) {
            super(from, to);
        }

        @Override
        public Type type() {
            return Type.HEARTBEAT_REPLY;
        }
    }

    public static class HeartbeatRequest extends GossipMessage {
        public HeartbeatRequest(ClusterAddress from, ClusterAddress to) {
            super(from, to);
        }

        @Override
        public Type type() {
            return Type.HEARTBEAT_REQUEST;
        }
    }

    public abstract static class JoinReply extends GossipMessage {
        public JoinReply(ClusterAddress from, ClusterAddress to) {
            super(from, to);
        }

        public abstract boolean isAccept();

        public abstract JoinAccept asAccept();

        public abstract JoinReject asReject();
    }

    public static class JoinAccept extends JoinReply {
        private final Gossip gossip;

        public JoinAccept(ClusterAddress from, ClusterAddress to, Gossip gossip) {
            super(from, to);

            this.gossip = gossip;
        }

        public Gossip gossip() {
            return gossip;
        }

        @Override
        public boolean isAccept() {
            return true;
        }

        @Override
        public JoinAccept asAccept() {
            return this;
        }

        @Override
        public JoinReject asReject() {
            throw new UnsupportedOperationException(getClass().getSimpleName()
                + " can't be used as " + JoinReject.class.getSimpleName());
        }

        @Override
        public Type type() {
            return Type.JOIN_ACCEPT;
        }
    }

    public static class JoinReject extends JoinReply {
        public enum RejectType {
            TEMPORARY,

            PERMANENT,

            FATAL,

            CONFLICT
        }

        private final RejectType rejectType;

        private final InetSocketAddress rejectedAddress;

        private final String reason;

        public JoinReject(ClusterAddress from, ClusterAddress to, InetSocketAddress rejectedAddress, RejectType rejectType,
            String reason) {
            super(from, to);

            this.rejectedAddress = rejectedAddress;
            this.rejectType = rejectType;
            this.reason = reason;
        }

        public static JoinReject permanent(ClusterAddress from, ClusterAddress to, InetSocketAddress rejectedAddress) {
            return new JoinReject(from, to, rejectedAddress, RejectType.PERMANENT, null);
        }

        public static JoinReject retryLater(ClusterAddress from, ClusterAddress to, InetSocketAddress rejectedAddress) {
            return new JoinReject(from, to, rejectedAddress, RejectType.TEMPORARY, null);
        }

        public static JoinReject fatal(ClusterAddress from, ClusterAddress to, InetSocketAddress rejectedAddress, String reason) {
            return new JoinReject(from, to, rejectedAddress, RejectType.FATAL, reason);
        }

        public static JoinReject conflict(ClusterAddress from, ClusterAddress to, InetSocketAddress rejectedAddress) {
            return new JoinReject(from, to, rejectedAddress, RejectType.CONFLICT, null);
        }

        public RejectType rejectType() {
            return rejectType;
        }

        public String reason() {
            return reason;
        }

        public InetSocketAddress rejectedAddress() {
            return rejectedAddress;
        }

        @Override
        public boolean isAccept() {
            return false;
        }

        @Override
        public JoinAccept asAccept() {
            throw new UnsupportedOperationException(getClass().getSimpleName()
                + " can't be used as " + JoinAccept.class.getSimpleName());
        }

        @Override
        public JoinReject asReject() {
            return this;
        }

        @Override
        public Type type() {
            return Type.JOIN_REJECT;
        }
    }

    public static class JoinRequest extends GossipProtocol {
        private final ClusterNode fromNode;

        private final InetSocketAddress to;

        private final String cluster;

        public JoinRequest(ClusterNode from, String cluster, InetSocketAddress to) {
            super(from.address());

            this.cluster = cluster;
            this.fromNode = from;
            this.to = to;
        }

        public ClusterNode fromNode() {
            return fromNode;
        }

        public String cluster() {
            return cluster;
        }

        @Override
        public InetSocketAddress toAddress() {
            return to;
        }

        @Override
        public Type type() {
            return Type.JOIN_REQUEST;
        }
    }

    public abstract static class UpdateBase extends GossipMessage {
        public UpdateBase(ClusterAddress from, ClusterAddress to) {
            super(from, to);
        }

        public abstract GossipBase gossipBase();

        public abstract Update asUpdate();
    }

    public static class Update extends UpdateBase {
        private final Gossip gossip;

        public Update(ClusterAddress from, ClusterAddress to, Gossip gossip) {
            super(from, to);

            this.gossip = gossip;
        }

        public Gossip gossip() {
            return gossip;
        }

        @Override
        public GossipBase gossipBase() {
            return gossip;
        }

        @Override
        public Update asUpdate() {
            return this;
        }

        @Override
        public Type type() {
            return Type.GOSSIP_UPDATE;
        }
    }

    public static class UpdateDigest extends UpdateBase {
        private final GossipDigest digest;

        public UpdateDigest(ClusterAddress from, ClusterAddress to, GossipDigest digest) {
            super(from, to);

            this.digest = digest;
        }

        public GossipDigest digest() {
            return digest;
        }

        @Override
        public GossipBase gossipBase() {
            return digest;
        }

        @Override
        public Update asUpdate() {
            return null;
        }

        @Override
        public Type type() {
            return Type.GOSSIP_UPDATE_DIGEST;
        }
    }

    private final ClusterAddress from;

    public GossipProtocol(ClusterAddress from) {
        this.from = from;
    }

    public abstract Type type();

    public abstract InetSocketAddress toAddress();

    public ClusterAddress from() {
        return from;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
