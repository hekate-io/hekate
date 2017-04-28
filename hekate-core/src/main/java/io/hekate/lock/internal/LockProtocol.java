/*
 * Copyright 2017 The Hekate Project
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
import io.hekate.cluster.ClusterUuid;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.List;
import java.util.Map;

abstract class LockProtocol {
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

    static class LockOwnerRequest extends LockRequestBase {
        public LockOwnerRequest(String region, String lockName) {
            this(region, lockName, null);
        }

        public LockOwnerRequest(String region, String lockName, ClusterHash topology) {
            super(region, lockName);

            setTopology(topology);
        }

        @Override
        public Type getType() {
            return Type.OWNER_REQUEST;
        }
    }

    static class LockOwnerResponse extends LockProtocol {
        public enum Status {
            OK,

            RETRY,
        }

        private final long threadId;

        private final ClusterUuid owner;

        private final LockOwnerResponse.Status status;

        public LockOwnerResponse(long threadId, ClusterUuid owner,
            LockOwnerResponse.Status status) {
            this.threadId = threadId;
            this.owner = owner;
            this.status = status;
        }

        public long getThreadId() {
            return threadId;
        }

        public ClusterUuid getOwner() {
            return owner;
        }

        public LockOwnerResponse.Status getStatus() {
            return status;
        }

        @Override
        public Type getType() {
            return Type.OWNER_RESPONSE;
        }
    }

    abstract static class LockRequestBase extends LockProtocol {
        private final String region;

        private final String lockName;

        private ClusterHash topology;

        public LockRequestBase(String region, String lockName) {
            this.region = region;
            this.lockName = lockName;
        }

        public String getRegion() {
            return region;
        }

        public String getLockName() {
            return lockName;
        }

        public ClusterHash getTopology() {
            return topology;
        }

        public void setTopology(ClusterHash topology) {
            this.topology = topology;
        }
    }

    static class LockRequest extends LockRequestBase implements LockIdentity {
        private final long lockId;

        private final ClusterUuid node;

        private final long timeout;

        private final boolean withFeedback;

        private final long threadId;

        public LockRequest(long lockId, String region, String lockName, ClusterUuid node, long timeout, boolean withFeedback,
            long threadId) {
            this(lockId, region, lockName, node, timeout, null, withFeedback, threadId);
        }

        public LockRequest(long lockId, String region, String lockName, ClusterUuid node, long timeout, ClusterHash topology,
            boolean withFeedback, long threadId) {
            super(region, lockName);

            this.lockId = lockId;
            this.node = node;
            this.timeout = timeout;
            this.withFeedback = withFeedback;
            this.threadId = threadId;

            setTopology(topology);
        }

        @Override
        public long getLockId() {
            return lockId;
        }

        @Override
        public ClusterUuid getNode() {
            return node;
        }

        public long getTimeout() {
            return timeout;
        }

        public boolean isWithFeedback() {
            return withFeedback;
        }

        @Override
        public long getThreadId() {
            return threadId;
        }

        @Override
        public Type getType() {
            return Type.LOCK_REQUEST;
        }
    }

    static class LockResponse extends LockProtocol {
        enum Status {
            OK,

            RETRY,

            TIMEOUT,

            REPLACED,

            BUSY,

            LOCK_INFO
        }

        private final LockResponse.Status status;

        private final ClusterUuid owner;

        private final long ownerThreadId;

        public LockResponse(LockResponse.Status status, ClusterUuid owner, long ownerThreadId) {
            this.status = status;
            this.owner = owner;
            this.ownerThreadId = ownerThreadId;
        }

        public LockResponse.Status getStatus() {
            return status;
        }

        public ClusterUuid getOwner() {
            return owner;
        }

        public long getOwnerThreadId() {
            return ownerThreadId;
        }

        @Override
        public Type getType() {
            return Type.LOCK_RESPONSE;
        }
    }

    static class UnlockRequest extends LockRequestBase implements LockIdentity {
        private final long lockId;

        private final ClusterUuid node;

        public UnlockRequest(long lockId, String region, String lockName, ClusterUuid node) {
            this(lockId, region, lockName, node, null);
        }

        public UnlockRequest(long lockId, String region, String lockName, ClusterUuid node, ClusterHash topology) {
            super(region, lockName);

            this.lockId = lockId;
            this.node = node;

            setTopology(topology);
        }

        @Override
        public long getLockId() {
            return lockId;
        }

        @Override
        public long getThreadId() {
            // Not used.
            return 0;
        }

        @Override
        public ClusterUuid getNode() {
            return node;
        }

        @Override
        public Type getType() {
            return Type.UNLOCK_REQUEST;
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

        public UnlockResponse.Status getStatus() {
            return status;
        }

        @Override
        public LockProtocol.Type getType() {
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

        public String getRegion() {
            return region;
        }

        public LockMigrationKey getKey() {
            return key;
        }
    }

    static class MigrationPrepareRequest extends MigrationRequest {
        private final boolean firstPass;

        private final Map<ClusterUuid, ClusterHash> topologies;

        @ToStringIgnore
        private final List<LockMigrationInfo> locks;

        public MigrationPrepareRequest(String region, LockMigrationKey key, boolean firstPass,
            Map<ClusterUuid, ClusterHash> topologies, List<LockMigrationInfo> locks) {
            super(region, key);

            this.firstPass = firstPass;
            this.topologies = topologies;
            this.locks = locks;
        }

        public boolean isFirstPass() {
            return firstPass;
        }

        public List<LockMigrationInfo> getLocks() {
            return locks;
        }

        public Map<ClusterUuid, ClusterHash> getTopologies() {
            return topologies;
        }

        @Override
        public Type getType() {
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

        public List<LockMigrationInfo> getLocks() {
            return locks;
        }

        @Override
        public Type getType() {
            return Type.MIGRATION_APPLY;
        }
    }

    static class MigrationResponse extends LockProtocol {
        public enum Status {
            OK,

            RETRY
        }

        private final MigrationResponse.Status status;

        public MigrationResponse(MigrationResponse.Status status) {
            this.status = status;
        }

        public MigrationResponse.Status getStatus() {
            return status;
        }

        @Override
        public Type getType() {
            return Type.MIGRATION_RESPONSE;
        }
    }

    public abstract Type getType();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
