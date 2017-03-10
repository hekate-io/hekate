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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterTopologyHash;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockResponse;
import io.hekate.lock.internal.LockProtocol.UnlockRequest;
import io.hekate.lock.internal.LockProtocol.UnlockResponse;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.unicast.Reply;
import io.hekate.messaging.unicast.ReplyDecision;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
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

    interface UnlockCallback {
        void onUnlock(LockControllerClient handle);
    }

    private static final Logger log = LoggerFactory.getLogger(LockControllerClient.class);

    private final String region;

    private final String name;

    private final long lockId;

    private final long threadId;

    private final ClusterNodeId node;

    private final long lockTimeout;

    @ToStringIgnore
    private final MessagingChannel<LockProtocol> channel;

    @ToStringIgnore
    private final ReentrantLock lock = new ReentrantLock();

    @ToStringIgnore
    private final UnlockCallback unlockCallback;

    @ToStringIgnore
    private final LockFuture lockFuture;

    @ToStringIgnore
    private final LockFuture unlockFuture;

    private final AsyncLockCallbackAdaptor callback;

    @ToStringIgnore
    private ClusterTopology topology;

    @ToStringIgnore
    private LockOwnerInfo lockOwner;

    @ToStringIgnore
    private ClusterNodeId managedBy;

    private Status status = Status.UNLOCKED;

    public LockControllerClient(long lockId, ClusterNodeId node, long threadId, DistributedLock lock,
        MessagingChannel<LockProtocol> channel, long lockTimeout, AsyncLockCallbackAdaptor callback, UnlockCallback unlockCallback) {
        assert node != null : "Cluster node is null.";
        assert lock != null : "Lock is null.";
        assert channel != null : "Channel is null.";
        assert unlockCallback != null : "Unlock callback is null.";

        this.region = lock.getRegion();
        this.name = lock.getName();
        this.lockId = lockId;
        this.node = node;
        this.threadId = threadId;
        this.unlockCallback = unlockCallback;
        this.lockTimeout = lockTimeout;
        this.callback = callback;

        // Make sure that all messages will be routed with the affinity key of this lock.
        this.channel = channel.withAffinityKey(new LockAffinityKey(lock.getRegion(), lock.getName()));

        lockFuture = new LockFuture(this);
        unlockFuture = new LockFuture(this);
    }

    public String getName() {
        return name;
    }

    public String getRegion() {
        return region;
    }

    public long getLockId() {
        return lockId;
    }

    public long getThreadId() {
        return threadId;
    }

    public ClusterNodeId getManagedBy() {
        lock.lock();

        try {
            return managedBy;
        } finally {
            lock.unlock();
        }
    }

    public LockFuture getLockFuture() {
        return lockFuture;
    }

    public LockFuture getUnlockFuture() {
        return unlockFuture;
    }

    public ClusterNodeId getNode() {
        return node;
    }

    public ClusterTopology getTopology() {
        lock.lock();

        try {
            return topology;
        } finally {
            lock.unlock();
        }
    }

    public void update(ClusterNodeId managedBy, ClusterTopology topology) {
        lock.lock();

        try {
            this.topology = topology;
            this.managedBy = managedBy;
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
        boolean wasLocked;

        lock.lock();

        try {
            wasLocked = status == Status.LOCKED;

            status = Status.TERMINATED;
        } finally {
            lock.unlock();
        }

        if (!lockFuture.isDone()) {
            if (cancel) {
                lockFuture.cancel(false);
            } else {
                lockFuture.completeExceptionally(new IllegalStateException("Lock service is not initialized."));
            }
        }

        if (wasLocked && callback != null) {
            callback.onLockRelease(this);
        }

        if (!unlockFuture.isDone()) {
            unlockFuture.complete(true);
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

    private boolean becomeLocked(ClusterTopologyHash requestTopology) {
        boolean complete = false;

        lock.lock();

        try {
            if (!requestTopology.equals(topology.getHash())) {
                return false;
            }

            switch (status) {
                case LOCKING: {
                    status = Status.LOCKED;

                    lockOwner = new DefaultLockOwnerInfo(threadId, topology.getLocalNode());

                    complete = true;

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

        // Notify out of synchronization block.
        if (complete) {
            lockFuture.complete(true);

            if (callback != null) {
                callback.onLockAcquire(this);
            }
        }

        return true;
    }

    private boolean notifyOnLockBusy(ClusterNodeId ownerId, long ownerThreadId, ClusterTopologyHash requestTopology) {
        if (callback != null) {
            lock.lock();

            try {
                if (!requestTopology.equals(topology.getHash())) {
                    return false;
                }

                if (status == Status.LOCKING) {
                    ClusterNode ownerNode = topology.get(ownerId);

                    LockOwnerInfo newOwner = new DefaultLockOwnerInfo(ownerThreadId, ownerNode);

                    if (lockOwner == null) {
                        lockOwner = newOwner;

                        callback.onLockBusy(newOwner);
                    } else if (!lockOwner.equals(newOwner)) {
                        lockOwner = newOwner;

                        callback.onLockOwnerChange(newOwner);
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

    private boolean becomeUnlocked(ClusterTopologyHash requestTopology) {
        Boolean complete = null;

        lock.lock();

        try {
            if (requestTopology != null && !requestTopology.equals(topology.getHash())) {
                return false;
            }

            switch (status) {
                case LOCKING: {
                    status = Status.UNLOCKED;

                    complete = false;

                    break;
                }
                case LOCKED: {
                    illegalStateTransition(Status.UNLOCKED);

                    break;
                }
                case UNLOCKING: {
                    status = Status.UNLOCKED;

                    complete = true;

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

        // Notify future out of synchronization block.
        if (complete != null) {
            unlockCallback.onUnlock(this);

            if (complete) {
                if (callback != null) {
                    callback.onLockRelease(this);
                }

                unlockFuture.complete(true);
            } else {
                lockFuture.complete(false);
            }
        }

        return true;
    }

    private void remoteLock() {
        boolean withFeedback = callback != null;

        LockRequest lockReq = new LockRequest(lockId, region, name, node, lockTimeout, withFeedback, threadId);

        channel.request(lockReq, new RequestCallback<LockProtocol>() {
            @Override
            public ReplyDecision accept(Throwable err, LockProtocol reply) {
                if (err == null) {
                    LockResponse lockRsp = (LockResponse)reply;

                    switch (lockRsp.getStatus()) {
                        case OK: {
                            if (becomeLocked(lockReq.getTopology())) {
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
                            if (notifyOnLockBusy(lockRsp.getOwner(), lockRsp.getOwnerThreadId(), lockReq.getTopology())) {
                                return COMPLETE;
                            } else {
                                // Retry if still LOCKING.
                                return is(Status.LOCKING) ? REJECT : COMPLETE;
                            }
                        }
                        default: {
                            throw new IllegalArgumentException("Unexpected status: " + lockRsp.getStatus());
                        }
                    }
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn("Failed to send lock message [error={}, message={}]", err.toString(), lockReq);
                    }

                    // Retry if still LOCKING.
                    return is(Status.LOCKING) ? REJECT : COMPLETE;
                }
            }

            @Override
            public void onComplete(Throwable err, Reply<LockProtocol> reply) {
                if (err != null && is(Status.LOCKING)) {
                    log.error("Failed to submit lock request [request={}]", lockReq, err);
                }
            }
        });
    }

    private void remoteUnlock() {
        UnlockRequest unlockReq = new UnlockRequest(lockId, region, name, node);

        channel.request(unlockReq, new RequestCallback<LockProtocol>() {
            @Override
            public ReplyDecision accept(Throwable err, LockProtocol reply) {
                if (err == null) {
                    UnlockResponse lockRsp = (UnlockResponse)reply;

                    switch (lockRsp.getStatus()) {
                        case OK: {
                            if (becomeUnlocked(unlockReq.getTopology())) {
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
                            throw new IllegalArgumentException("Unexpected status: " + lockRsp.getStatus());
                        }
                    }
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn("Failed to send lock message [error={}, message={}]", err.toString(), unlockReq);
                    }

                    // Retry if not TERMINATED.
                    return !is(Status.TERMINATED) ? REJECT : COMPLETE;
                }
            }

            @Override
            public void onComplete(Throwable err, Reply<LockProtocol> reply) {
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
