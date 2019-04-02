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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.hekate.util.trace.TraceInfo;
import io.hekate.util.trace.Traceable;
import java.util.List;
import java.util.Map;

abstract class LockProtocol implements Traceable {
    enum Type {
        LOCK_REQUEST,

        LOCK_RESPONSE,

        UNLOCK_REQUEST,

        UNLOCK_RESPONSE,

        OWNER_REQUEST,

        OWNER_RESPONSE,

        MIGRATION_PREPARE,

        MIGRATION_APPLY,

        MIGRATION_RESPONSE,
    }

    abstract static class LockRequestBase extends LockProtocol {
        private final String region;

        private final String lockName;

        @ToStringIgnore
        private final ClusterHash topology;

        public LockRequestBase(String region, String lockName, ClusterHash topology) {
            this.region = region;
            this.lockName = lockName;
            this.topology = topology;
        }

        public abstract LockRequestBase withTopology(ClusterHash topology);

        public String region() {
            return region;
        }

        public String lockName() {
            return lockName;
        }

        public ClusterHash topology() {
            return topology;
        }
    }

    static class LockOwnerRequest extends LockRequestBase {
        public LockOwnerRequest(String region, String lockName) {
            this(region, lockName, null);
        }

        public LockOwnerRequest(String region, String lockName, ClusterHash topology) {
            super(region, lockName, topology);
        }

        @Override
        public LockRequestBase withTopology(ClusterHash topology) {
            return new LockOwnerRequest(region(), lockName(), topology);
        }

        @Override
        public Type type() {
            return Type.OWNER_REQUEST;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(region() + "/get-lock-owner")
                .withTag("lock-region", region())
                .withTag("lock-name", lockName());
        }
    }

    static class LockOwnerResponse extends LockProtocol {
        enum Status {
            OK,

            RETRY,
        }

        private final long threadId;

        private final ClusterNodeId owner;

        private final LockOwnerResponse.Status status;

        public LockOwnerResponse(long threadId, ClusterNodeId owner, LockOwnerResponse.Status status) {
            this.threadId = threadId;
            this.owner = owner;
            this.status = status;
        }

        public long threadId() {
            return threadId;
        }

        public ClusterNodeId owner() {
            return owner;
        }

        public LockOwnerResponse.Status status() {
            return status;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(status.name())
                .withTag("owner", owner);
        }

        @Override
        public Type type() {
            return Type.OWNER_RESPONSE;
        }
    }

    static class LockRequest extends LockRequestBase implements LockIdentity {
        private final long lockId;

        private final ClusterNodeId node;

        private final long timeout;

        private final long threadId;

        public LockRequest(long lockId, String region, String lockName, ClusterNodeId node, long timeout, long threadId) {
            this(lockId, region, lockName, node, timeout, null, threadId);
        }

        public LockRequest(long lockId, String region, String lockName, ClusterNodeId node, long timeout, ClusterHash hash, long threadId) {
            super(region, lockName, hash);

            this.lockId = lockId;
            this.node = node;
            this.timeout = timeout;
            this.threadId = threadId;
        }

        @Override
        public long lockId() {
            return lockId;
        }

        @Override
        public ClusterNodeId node() {
            return node;
        }

        public long timeout() {
            return timeout;
        }

        @Override
        public long threadId() {
            return threadId;
        }

        @Override
        public Type type() {
            return Type.LOCK_REQUEST;
        }

        @Override
        public LockRequestBase withTopology(ClusterHash topology) {
            return new LockRequest(lockId, region(), lockName(), node, timeout, topology, threadId);
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(region() + "/lock")
                .withTag("lock-region", region())
                .withTag("lock-name", lockName());
        }
    }

    static class LockResponse extends LockProtocol {
        enum Status {
            OK,

            RETRY,

            LOCK_TIMEOUT,

            LOCK_BUSY,

            LOCK_OWNER_CHANGE
        }

        private final LockResponse.Status status;

        private final ClusterNodeId owner;

        private final long ownerThreadId;

        public LockResponse(LockResponse.Status status, ClusterNodeId owner, long ownerThreadId) {
            this.status = status;
            this.owner = owner;
            this.ownerThreadId = ownerThreadId;
        }

        public LockResponse.Status status() {
            return status;
        }

        public ClusterNodeId owner() {
            return owner;
        }

        public long ownerThreadId() {
            return ownerThreadId;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(status.name());
        }

        @Override
        public Type type() {
            return Type.LOCK_RESPONSE;
        }
    }

    static class UnlockRequest extends LockRequestBase implements LockIdentity {
        private final long lockId;

        private final ClusterNodeId node;

        public UnlockRequest(long lockId, String region, String lockName, ClusterNodeId node) {
            this(lockId, region, lockName, node, null);
        }

        public UnlockRequest(long lockId, String region, String lockName, ClusterNodeId node, ClusterHash topology) {
            super(region, lockName, topology);

            this.lockId = lockId;
            this.node = node;
        }

        @Override
        public long lockId() {
            return lockId;
        }

        @Override
        public long threadId() {
            // Not used.
            return 0;
        }

        @Override
        public ClusterNodeId node() {
            return node;
        }

        @Override
        public Type type() {
            return Type.UNLOCK_REQUEST;
        }

        @Override
        public LockRequestBase withTopology(ClusterHash topology) {
            return new UnlockRequest(lockId, region(), lockName(), node, topology);
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(region() + "/unlock")
                .withTag("lock-region", region())
                .withTag("lock-name", lockName());
        }
    }

    static class UnlockResponse extends LockProtocol {
        enum Status {
            OK,

            RETRY,
        }

        private final UnlockResponse.Status status;

        public UnlockResponse(UnlockResponse.Status status) {
            this.status = status;
        }

        public UnlockResponse.Status status() {
            return status;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(status.name());
        }

        @Override
        public LockProtocol.Type type() {
            return Type.UNLOCK_RESPONSE;
        }
    }

    abstract static class MigrationRequest extends LockProtocol {
        private final String region;

        private final LockMigrationKey key;

        public MigrationRequest(String region, LockMigrationKey key) {
            this.region = region;
            this.key = key;
        }

        public String region() {
            return region;
        }

        public LockMigrationKey key() {
            return key;
        }
    }

    static class MigrationPrepareRequest extends MigrationRequest {
        private final boolean firstPass;

        private final Map<ClusterNodeId, ClusterHash> topologies;

        @ToStringIgnore
        private final List<LockMigrationInfo> locks;

        public MigrationPrepareRequest(
            String region,
            LockMigrationKey key,
            boolean firstPass,
            Map<ClusterNodeId, ClusterHash> topologies,
            List<LockMigrationInfo> locks
        ) {
            super(region, key);

            this.firstPass = firstPass;
            this.topologies = topologies;
            this.locks = locks;
        }

        public boolean isFirstPass() {
            return firstPass;
        }

        public List<LockMigrationInfo> locks() {
            return locks;
        }

        public Map<ClusterNodeId, ClusterHash> topologies() {
            return topologies;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(region() + "/migration/prepare")
                .withTag("lock-region", region())
                .withTag("locks", locks.size())
                .withTag("first-pass", firstPass);
        }

        @Override
        public Type type() {
            return Type.MIGRATION_PREPARE;
        }
    }

    static class MigrationApplyRequest extends MigrationRequest {
        @ToStringIgnore
        private final List<LockMigrationInfo> locks;

        public MigrationApplyRequest(String region, LockMigrationKey key, List<LockMigrationInfo> locks) {
            super(region, key);

            this.locks = locks;
        }

        public List<LockMigrationInfo> locks() {
            return locks;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(region() + "/migration/apply")
                .withTag("lock-region", region())
                .withTag("locks", locks.size());
        }

        @Override
        public Type type() {
            return Type.MIGRATION_APPLY;
        }
    }

    static class MigrationResponse extends LockProtocol {
        enum Status {
            OK,

            RETRY
        }

        private final MigrationResponse.Status status;

        public MigrationResponse(MigrationResponse.Status status) {
            this.status = status;
        }

        public MigrationResponse.Status status() {
            return status;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(status.name());
        }

        @Override
        public Type type() {
            return Type.MIGRATION_RESPONSE;
        }
    }

    public abstract Type type();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
