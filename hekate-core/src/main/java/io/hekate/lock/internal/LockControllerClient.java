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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockResponse;
import io.hekate.lock.internal.LockProtocol.UnlockRequest;
import io.hekate.lock.internal.LockProtocol.UnlockResponse;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.CancellationException;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.messaging.unicast.ReplyDecision.COMPLETE;
import static io.hekate.messaging.unicast.ReplyDecision.REJECT;

class LockControllerClient {
    enum Status {
        LOCKING,

        LOCKED,

        UNLOCKING,

        UNLOCKED,

        TERMINATED
    }

    interface CleanupCallback {
        void cleanup(LockControllerClient lock);
    }

    private static final Logger log = LoggerFactory.getLogger(LockControllerClient.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final LockKey key;

    private final long lockId;

    private final long threadId;

    private final ClusterNodeId localNode;

    private final long lockTimeout;

    @ToStringIgnore
    private final MessagingChannel<LockProtocol> channel;

    @ToStringIgnore
    private final ReentrantLock lock = new ReentrantLock();

    @ToStringIgnore
    private final CleanupCallback cleanupCallback;

    @ToStringIgnore
    private final LockFuture lockFuture;

    @ToStringIgnore
    private final LockFuture unlockFuture;

    @ToStringIgnore
    private final LockRegionMetrics metrics;

    @ToStringIgnore
    private final AsyncLockCallbackAdaptor asyncCallback;

    @ToStringIgnore
    private ClusterTopology topology;

    @ToStringIgnore
    private LockOwnerInfo lockOwner;

    @ToStringIgnore
    private ClusterNodeId manager;

    private Status status = Status.UNLOCKED;

    public LockControllerClient(
        long lockId,
        ClusterNodeId localNode,
        long threadId,
        DistributedLock lock,
        MessagingChannel<LockProtocol> channel,
        long lockTimeout,
        LockRegionMetrics metrics,
        AsyncLockCallbackAdaptor asyncCallback,
        CleanupCallback cleanupCallback
    ) {
        assert localNode != null : "Cluster node is null.";
        assert lock != null : "Lock is null.";
        assert channel != null : "Channel is null.";
        assert metrics != null : "Metrics are null.";
        assert cleanupCallback != null : "Cleanup callback is null.";

        this.key = new LockKey(lock.regionName(), lock.name());
        this.lockId = lockId;
        this.localNode = localNode;
        this.threadId = threadId;
        this.lockTimeout = lockTimeout;
        this.metrics = metrics;
        this.cleanupCallback = cleanupCallback;
        this.asyncCallback = asyncCallback;

        // Make sure that all messages will be routed with the affinity key of this lock.
        this.channel = channel.withAffinity(key);

        lockFuture = new LockFuture(this);
        unlockFuture = new LockFuture(this);
    }

    public LockKey key() {
        return key;
    }

    public long lockId() {
        return lockId;
    }

    public long threadId() {
        return threadId;
    }

    public ClusterNodeId manager() {
        lock.lock();

        try {
            return manager;
        } finally {
            lock.unlock();
        }
    }

    public LockFuture lockFuture() {
        return lockFuture;
    }

    public LockFuture unlockFuture() {
        return unlockFuture;
    }

    public ClusterNodeId localNode() {
        return localNode;
    }

    public void update(PartitionMapper mapping) {
        lock.lock();

        try {
            this.topology = mapping.topology();

            this.manager = mapping.map(key).primaryNode().id();
        } finally {
            lock.unlock();
        }
    }

    public boolean updateAndCheckLocked(ClusterTopology topology) {
        lock.lock();

        try {
            this.topology = topology;

            return status == Status.LOCKED;
        } finally {
            lock.unlock();
        }
    }

    public void becomeLocking() {
        lock.lock();

        try {
            assert status == Status.UNLOCKED;

            status = Status.LOCKING;

            remoteLock();
        } finally {
            lock.unlock();
        }
    }

    public void becomeUnlocking() {
        doBecomeUnlocking(false);
    }

    public void becomeUnlockingIfNotLocked() {
        doBecomeUnlocking(true);
    }

    public void becomeTerminated(boolean cancel) {
        lock.lock();

        try {
            status = Status.TERMINATED;

            if (!lockFuture.isDone()) {
                if (cancel) {
                    lockFuture.cancel(false);
                } else {
                    lockFuture.completeExceptionally(new CancellationException("Lock service terminated."));
                }
            }

            if (unlockFuture.complete(true)) {
                metrics.onUnlock();

                if (asyncCallback != null) {
                    asyncCallback.onLockRelease();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void doBecomeUnlocking(boolean ignoreIfLocked) {
        lock.lock();

        try {
            switch (status) {
                case LOCKING: {
                    status = Status.UNLOCKING;

                    if (!lockFuture.isDone()) {
                        lockFuture.complete(false);
                    }

                    remoteUnlock();

                    break;
                }
                case LOCKED: {
                    if (!ignoreIfLocked) {
                        status = Status.UNLOCKING;

                        remoteUnlock();
                    }

                    break;
                }
                case UNLOCKING: {
                    // No-op.
                    break;
                }
                case UNLOCKED: {
                    // No-op.
                    break;
                }
                case TERMINATED: {
                    // No-op.
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected lock status: " + status);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean becomeLocked(ClusterHash requestTopology) {
        lock.lock();

        try {
            if (!requestTopology.equals(topology.hash())) {
                return false;
            }

            switch (status) {
                case LOCKING: {
                    status = Status.LOCKED;

                    lockOwner = new DefaultLockOwnerInfo(threadId, topology.localNode());

                    metrics.onLock();

                    lockFuture.complete(true);

                    if (asyncCallback != null) {
                        asyncCallback.onLockAcquire(this);
                    }

                    break;
                }
                case LOCKED: {
                    // No-op.
                    break;
                }
                case UNLOCKING: {
                    // No-op.
                    break;
                }
                case UNLOCKED: {
                    remoteUnlock();

                    break;
                }
                case TERMINATED: {
                    // No-op.
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected lock status: " + status);
                }
            }
        } finally {
            lock.unlock();
        }

        return true;
    }

    private boolean notifyOnLockBusy(ClusterNodeId ownerId, long ownerThreadId, ClusterHash requestTopology) {
        if (asyncCallback != null) {
            lock.lock();

            try {
                if (!requestTopology.equals(topology.hash())) {
                    return false;
                }

                if (status == Status.LOCKING) {
                    ClusterNode ownerNode = topology.get(ownerId);

                    LockOwnerInfo newOwner = new DefaultLockOwnerInfo(ownerThreadId, ownerNode);

                    if (lockOwner == null) {
                        lockOwner = newOwner;

                        asyncCallback.onLockBusy(newOwner);
                    } else if (!lockOwner.equals(newOwner)) {
                        lockOwner = newOwner;

                        asyncCallback.onLockOwnerChange(newOwner);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        return true;
    }

    private void becomeUnlocked() {
        becomeUnlocked(null);
    }

    private boolean becomeUnlocked(ClusterHash requestTopology) {
        boolean cleanup = false;

        lock.lock();

        try {
            if (requestTopology != null && !requestTopology.equals(topology.hash())) {
                return false;
            }

            switch (status) {
                case LOCKING: {
                    status = Status.UNLOCKED;

                    cleanup = true;

                    lockFuture.complete(false);

                    break;
                }
                case LOCKED: {
                    illegalStateTransition(Status.UNLOCKED);

                    break;
                }
                case UNLOCKING: {
                    status = Status.UNLOCKED;

                    metrics.onUnlock();

                    cleanup = true;

                    unlockFuture.complete(true);

                    if (asyncCallback != null) {
                        asyncCallback.onLockRelease();
                    }

                    break;
                }
                case UNLOCKED: {
                    // No-op.
                    break;
                }
                case TERMINATED: {
                    // No-op.
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected lock status: " + status);
                }
            }
        } finally {
            lockOwner = null;

            lock.unlock();
        }

        // Cleanup out of synchronization block.
        if (cleanup) {
            cleanupCallback.cleanup(this);
        }

        return true;
    }

    private void remoteLock() {
        LockRequest lockReq = new LockRequest(lockId, key.region(), key.name(), localNode, lockTimeout, threadId);

        ResponseCallback<LockProtocol> callback = new ResponseCallback<LockProtocol>() {
            @Override
            public ReplyDecision accept(Throwable err, Response<LockProtocol> reply) {
                if (err == null) {
                    LockResponse lockRsp = reply.get(LockResponse.class);

                    switch (lockRsp.status()) {
                        case OK: {
                            ClusterHash topology = reply.topology().hash();

                            if (becomeLocked(topology)) {
                                return COMPLETE;
                            } else {
                                // Retry if still LOCKING.
                                return is(Status.LOCKING) ? REJECT : COMPLETE;
                            }
                        }
                        case RETRY: {
                            // Retry if still LOCKING.
                            return is(Status.LOCKING) ? REJECT : COMPLETE;
                        }
                        case TIMEOUT: {
                            becomeUnlocked();

                            return COMPLETE;
                        }
                        case REPLACED: {
                            return COMPLETE;
                        }
                        case BUSY: {
                            becomeUnlocked();

                            return COMPLETE;
                        }
                        case LOCK_INFO: {
                            ClusterHash topology = reply.topology().hash();

                            if (notifyOnLockBusy(lockRsp.owner(), lockRsp.ownerThreadId(), topology)) {
                                return COMPLETE;
                            } else {
                                // Retry if still LOCKING.
                                return is(Status.LOCKING) ? REJECT : COMPLETE;
                            }
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected status: " + lockRsp.status());
                        }
                    }
                } else {
                    if (DEBUG) {
                        log.debug("Failed to send lock message [error={}, message={}]", err.toString(), lockReq);
                    }

                    // Retry if still LOCKING.
                    return is(Status.LOCKING) ? REJECT : COMPLETE;
                }
            }

            @Override
            public void onComplete(Throwable err, Response<LockProtocol> rsp) {
                if (err != null && is(Status.LOCKING)) {
                    log.error("Failed to submit lock request [request={}]", lockReq, err);
                }
            }
        };

        if (asyncCallback == null) {
            // Send single request if we don't need to subscribe for updates.
            channel.request(lockReq, callback);
        } else {
            // Send subscription request if we need to receive lock owner updates.
            channel.subscribe(lockReq, callback);
        }
    }

    private void remoteUnlock() {
        UnlockRequest unlockReq = new UnlockRequest(lockId, key.region(), key.name(), localNode);

        channel.request(unlockReq, new ResponseCallback<LockProtocol>() {
            @Override
            public ReplyDecision accept(Throwable err, Response<LockProtocol> reply) {
                if (err == null) {
                    UnlockResponse lockRsp = reply.get(UnlockResponse.class);

                    switch (lockRsp.status()) {
                        case OK: {
                            ClusterHash topology = reply.topology().hash();

                            if (becomeUnlocked(topology)) {
                                return COMPLETE;
                            } else {
                                return REJECT;
                            }
                        }
                        case RETRY: {
                            // Retry if not TERMINATED.
                            return !is(Status.TERMINATED) ? REJECT : COMPLETE;
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected status: " + lockRsp.status());
                        }
                    }
                } else {
                    if (DEBUG) {
                        log.debug("Failed to send lock message [error={}, message={}]", err.toString(), unlockReq);
                    }

                    // Retry if not TERMINATED.
                    return !is(Status.TERMINATED) ? REJECT : COMPLETE;
                }
            }

            @Override
            public void onComplete(Throwable err, Response<LockProtocol> rsp) {
                if (err != null && !is(Status.TERMINATED)) {
                    log.error("Failed to submit unlock request [request={}]", unlockReq, err);
                }
            }
        });
    }

    private boolean is(Status status) {
        lock.lock();

        try {
            return this.status == status;
        } finally {
            lock.unlock();
        }
    }

    private void illegalStateTransition(Status newStatus) {
        throw new IllegalStateException("Illegal lock state transition from " + status + " to " + newStatus);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
