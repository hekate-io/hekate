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
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.lock.internal.LockProtocol.LockOwnerRequest;
import io.hekate.lock.internal.LockProtocol.LockOwnerResponse;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockResponse;
import io.hekate.lock.internal.LockProtocol.UnlockRequest;
import io.hekate.lock.internal.LockProtocol.UnlockResponse;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.operation.RequestRetryConfigurer;
import io.hekate.messaging.operation.Response;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.CancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LockControllerClient {
    enum Status {
        LOCKING,

        LOCKED,

        UNLOCKING,

        UNLOCKED,

        TERMINATED
    }

    private static final Logger log = LoggerFactory.getLogger(LockControllerClient.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final boolean TRACE = log.isTraceEnabled();

    private final LockKey key;

    private final long lockId;

    private final long threadId;

    private final ClusterNodeId localNode;

    private final long lockTimeout;

    @ToStringIgnore
    private final MessagingChannel<LockProtocol> channel;

    @ToStringIgnore
    private final Object mux = new Object();

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
        String region,
        String name,
        ClusterNodeId localNode,
        long threadId,
        MessagingChannel<LockProtocol> channel,
        long lockTimeout,
        LockRegionMetrics metrics,
        AsyncLockCallbackAdaptor asyncCallback
    ) {
        assert localNode != null : "Cluster node is null.";
        assert region != null : "Lock region is null.";
        assert name != null : "Lock name is null.";
        assert channel != null : "Channel is null.";
        assert metrics != null : "Metrics are null.";

        this.key = new LockKey(region, name);
        this.lockId = lockId;
        this.localNode = localNode;
        this.threadId = threadId;
        this.channel = channel;
        this.lockTimeout = lockTimeout;
        this.metrics = metrics;
        this.asyncCallback = asyncCallback;

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
        synchronized (mux) {
            return manager;
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
        if (mapping != null) {
            synchronized (mux) {
                this.topology = mapping.topology();

                this.manager = mapping.map(key).primaryNode().id();

                if (TRACE) {
                    log.trace("Updated partition mapping [key={}, manager={}, topology={}]", key, manager, topology);
                }
            }
        }
    }

    public boolean updateAndCheckLocked(ClusterTopology topology) {
        synchronized (mux) {
            this.topology = topology;

            if (DEBUG) {
                log.trace("Updated topology [key={}, topology={}]", key, topology);
            }

            return status == Status.LOCKED;
        }
    }

    public void becomeLocking(PartitionMapper mapping) {
        synchronized (mux) {
            assert status == Status.UNLOCKED;

            status = Status.LOCKING;

            if (DEBUG) {
                log.debug("Became {} [key={}]", status, key);
            }

            update(mapping);

            remoteLock();
        }
    }

    public LockFuture becomeUnlocking() {
        doBecomeUnlocking(false);

        return unlockFuture;
    }

    public void becomeUnlockingIfNotLocked() {
        doBecomeUnlocking(true);
    }

    public void becomeTerminated() {
        synchronized (mux) {
            status = Status.TERMINATED;

            if (DEBUG) {
                log.debug("Became {} [key={}]", status, key);
            }

            if (!lockFuture.isDone()) {
                lockFuture.completeExceptionally(new CancellationException("Lock service terminated."));
            }

            if (unlockFuture.complete(true)) {
                metrics.onUnlock();

                if (asyncCallback != null) {
                    asyncCallback.onLockRelease();
                }
            }
        }
    }

    private void doBecomeUnlocking(boolean ignoreIfLocked) {
        synchronized (mux) {
            switch (status) {
                case LOCKING: {
                    status = Status.UNLOCKING;

                    if (DEBUG) {
                        log.debug("Became {} [key={}]", status, key);
                    }

                    if (!lockFuture.isDone()) {
                        lockFuture.complete(false);
                    }

                    remoteUnlock();

                    break;
                }
                case LOCKED: {
                    if (!ignoreIfLocked) {
                        status = Status.UNLOCKING;

                        if (DEBUG) {
                            log.debug("Became {} [key={}]", status, key);
                        }

                        remoteUnlock();
                    }

                    break;
                }
                case UNLOCKING:
                case UNLOCKED:
                case TERMINATED: {
                    // No-op.
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected lock status: " + status);
                }
            }
        }
    }

    private boolean becomeLocked(ClusterHash requestTopology) {
        synchronized (mux) {
            if (topology == null || !requestTopology.equals(topology.hash())) {
                if (TRACE) {
                    log.trace("Rejected to become {} [key={}, topology={}]", Status.LOCKED, key, topology);
                }

                return false;
            }

            switch (status) {
                case LOCKING: {
                    status = Status.LOCKED;

                    lockOwner = new DefaultLockOwnerInfo(threadId, topology.localNode());

                    if (DEBUG) {
                        log.debug("Became {} [key={}]", status, key);
                    }

                    metrics.onLock();

                    lockFuture.complete(true);

                    if (asyncCallback != null) {
                        asyncCallback.onLockAcquire(this);
                    }

                    break;
                }
                case UNLOCKED: {
                    remoteUnlock();

                    break;
                }
                case LOCKED:
                case UNLOCKING:
                case TERMINATED: {
                    // No-op.
                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected lock status: " + status);
                }
            }
        }

        return true;
    }

    private void becomeUnlocked() {
        doBecomeUnlocked();
    }

    private boolean tryBecomeUnlocked(ClusterHash requestTopology) {
        synchronized (mux) {
            if (topology == null || (requestTopology != null && !requestTopology.equals(topology.hash()))) {
                if (TRACE) {
                    log.trace("Rejected to become {} [key={}, topology={}]", Status.UNLOCKED, key, topology);
                }

                return false;
            } else {
                doBecomeUnlocked();

                return true;
            }
        }
    }

    private void doBecomeUnlocked() {
        synchronized (mux) {
            try {
                switch (status) {
                    case LOCKING: {
                        status = Status.UNLOCKED;

                        if (DEBUG) {
                            log.debug("Became {} [key={}]", status, key);
                        }

                        lockFuture.complete(false);

                        break;
                    }
                    case LOCKED: {
                        illegalStateTransition(Status.UNLOCKED);

                        break;
                    }
                    case UNLOCKING: {
                        status = Status.UNLOCKED;

                        if (DEBUG) {
                            log.debug("Became {} [key={}]", status, key);
                        }

                        metrics.onUnlock();

                        unlockFuture.complete(true);

                        if (asyncCallback != null) {
                            asyncCallback.onLockRelease();
                        }

                        break;
                    }
                    case UNLOCKED:
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
            }
        }
    }

    private void remoteLock() {
        LockRequest lockReq = new LockRequest(lockId, key.region(), key.name(), localNode, lockTimeout, threadId);

        // Retry policy.
        RequestRetryConfigurer<LockProtocol> retryPolicy = retry -> retry
            .unlimitedAttempts()
            .alwaysReRoute()
            .whileTrue(() -> is(Status.LOCKING))
            .whileResponse(rsp -> {
                LockResponse lockRsp = rsp.payload(LockResponse.class);

                switch (lockRsp.status()) {
                    case OK: {
                        ClusterHash topology = rsp.topology().hash();

                        return !becomeLocked(topology);
                    }
                    case RETRY: {
                        return true;
                    }
                    case LOCK_TIMEOUT:
                    case LOCK_BUSY: {
                        becomeUnlocked();

                        return false;
                    }
                    case LOCK_OWNER_CHANGE: {
                        throw new IllegalArgumentException("Got an unexpected lock owner update message: " + rsp);
                    }
                    default: {
                        throw new IllegalArgumentException("Unexpected status: " + lockRsp.status());
                    }
                }
            });

        if (asyncCallback == null) {
            if (DEBUG) {
                log.debug("Submitting lock request [request={}]", lockReq);
            }

            // Send single request if we don't need to subscribe for updates.
            channel.newRequest(lockReq)
                .withAffinity(key)
                .withRetry(retryPolicy)
                .submit((err, rsp) -> {
                    if (err != null && is(Status.LOCKING)) {
                        log.error("Failed to submit lock request [request={}]", lockReq, err);
                    }
                });
        } else {
            if (DEBUG) {
                log.debug("Submitting lock subscription [request={}]", lockReq);
            }

            // Send subscription request if we need to receive lock owner updates.
            channel.newSubscribe(lockReq)
                .withAffinity(key)
                .withRetry(retryPolicy)
                .submit((err, rsp) -> {
                    if (err == null) {
                        LockResponse lockRsp = rsp.payload(LockResponse.class);

                        if (lockRsp.status() == LockResponse.Status.LOCK_OWNER_CHANGE) {
                            processLockOwnerChange(lockRsp, rsp);
                        }
                    } else if (is(Status.LOCKING)) {
                        log.error("Failed to submit lock request [request={}]", lockReq, err);
                    }
                });
        }
    }

    private void remoteUnlock() {
        UnlockRequest unlockReq = new UnlockRequest(lockId, key.region(), key.name(), localNode);

        if (DEBUG) {
            log.debug("Submitting unlock request [request={}]", unlockReq);
        }

        channel.newRequest(unlockReq)
            .withAffinity(key)
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .alwaysReRoute()
                .whileTrue(() -> is(Status.UNLOCKING))
                .whileResponse(rsp -> {
                    UnlockResponse unlockRsp = rsp.payload(UnlockResponse.class);

                    return unlockRsp.status() != UnlockResponse.Status.OK
                        || !tryBecomeUnlocked(rsp.topology().hash());
                })
            )
            .submit((err, rsp) -> {
                if (err != null && is(Status.UNLOCKING)) {
                    log.error("Failed to submit unlock request [request={}]", unlockReq, err);
                }
            });
    }

    private void processLockOwnerChange(LockResponse lockRsp, Response<LockProtocol> msg) {
        boolean notified = tryNotifyOnLockOwnerChange(lockRsp.owner(), lockRsp.ownerThreadId(), msg.topology().hash());

        if (!notified) {
            if (DEBUG) {
                log.debug("Sending explicit lock owner query [to={}, key={}]", msg.from(), key);
            }

            LockOwnerRequest req = new LockOwnerRequest(key.region(), key.name());

            channel.newRequest(req)
                .withAffinity(key)
                .withRetry(retry -> retry
                    .unlimitedAttempts()
                    .alwaysReRoute()
                    .whileTrue(() -> is(Status.LOCKING))
                    .whileResponse(rsp -> {
                        LockOwnerResponse ownerRsp = rsp.payload(LockOwnerResponse.class);

                        // Retry if update got rejected.
                        return ownerRsp.status() != LockOwnerResponse.Status.OK
                            || !tryNotifyOnLockOwnerChange(ownerRsp.owner(), ownerRsp.threadId(), rsp.topology().hash());
                    })
                )
                .submit((err, rsp) -> {
                    if (err != null && is(Status.LOCKING)) {
                        log.error("Failed to submit explicit lock owner query [request={}]", req, err);
                    }
                });
        }
    }

    private boolean tryNotifyOnLockOwnerChange(ClusterNodeId ownerId, long ownerThreadId, ClusterHash requestTopology) {
        synchronized (mux) {
            if (status != Status.LOCKING) {
                if (TRACE) {
                    log.trace("Ignored lock owner change because status is not {} [key={}, status={}]", Status.LOCKING, key, status);
                }

                // Not locking anymore (should not retry).
                return true;
            } else if (topology == null || !requestTopology.equals(topology.hash())) {
                if (TRACE) {
                    log.trace("Ignored lock owner change because of topology mismatch [key={}, topology={}]", key, topology);
                }

                // Should retry.
                return false;
            } else {
                ClusterNode ownerNode = topology.get(ownerId);

                LockOwnerInfo newOwner = new DefaultLockOwnerInfo(ownerThreadId, ownerNode);

                if (lockOwner == null) {
                    if (DEBUG) {
                        log.debug("Set initial lock owner [key={}, owner={}]", key, newOwner);
                    }

                    lockOwner = newOwner;

                    asyncCallback.onLockBusy(newOwner);
                } else if (!lockOwner.equals(newOwner)) {
                    if (DEBUG) {
                        log.debug("Updated lock owner [key={}, owner={}]", key, newOwner);
                    }

                    lockOwner = newOwner;

                    asyncCallback.onLockOwnerChange(newOwner);
                }

                // Successfully updated (should not retry).
                return true;
            }
        }
    }

    private boolean is(Status status) {
        synchronized (mux) {
            return this.status == status;
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
