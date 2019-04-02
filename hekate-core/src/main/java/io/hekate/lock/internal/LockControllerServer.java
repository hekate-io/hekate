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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.lock.internal.LockProtocol.LockOwnerResponse;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockResponse;
import io.hekate.lock.internal.LockProtocol.UnlockRequest;
import io.hekate.lock.internal.LockProtocol.UnlockResponse;
import io.hekate.messaging.Message;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

class LockControllerServer {
    private static class LockHolder implements LockIdentity {
        private final ClusterNodeId node;

        private final long lockId;

        private final long threadId;

        public LockHolder(ClusterNodeId node, long lockId, long threadId) {
            this.node = node;
            this.lockId = lockId;
            this.threadId = threadId;
        }

        @Override
        public ClusterNodeId node() {
            return node;
        }

        @Override
        public long threadId() {
            return threadId;
        }

        @Override
        public long lockId() {
            return lockId;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static class LockQueueEntry {
        private final Message<LockProtocol> message;

        @ToStringIgnore
        private final LockRequest request;

        @ToStringIgnore
        private final ScheduledFuture<?> timeoutFuture;

        public LockQueueEntry(Message<LockProtocol> message, LockRequest request, ScheduledFuture<?> timeoutFuture) {
            this.message = message;
            this.request = request;
            this.timeoutFuture = timeoutFuture;
        }

        public Message<LockProtocol> message() {
            return message;
        }

        public LockRequest request() {
            return request;
        }

        public ScheduledFuture<?> timeoutFuture() {
            return timeoutFuture;
        }

        public void cancelTimeout() {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
        }

        @Override
        public String toString() {
            return "QueueEntry[from=" + message.endpoint() + ", request=" + message + ']';
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LockControllerServer.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final boolean TRACE = log.isTraceEnabled();

    private final String name;

    private final ArrayDeque<LockQueueEntry> queue = new ArrayDeque<>();

    @ToStringIgnore
    private final ReentrantLock sync = new ReentrantLock();

    @ToStringIgnore
    private final ScheduledExecutorService scheduler;

    @ToStringIgnore
    private int busy;

    private LockHolder lockedOwner;

    public LockControllerServer(String name, ScheduledExecutorService scheduler) {
        assert name != null : "Name is null.";
        assert scheduler != null : "Scheduler is null.";

        this.scheduler = scheduler;
        this.name = name;
    }

    public void checkOut() {
        sync.lock();

        try {
            busy++;
        } finally {
            sync.unlock();
        }
    }

    public void checkIn() {
        sync.lock();

        try {
            busy--;
        } finally {
            sync.unlock();
        }
    }

    public boolean isFree() {
        sync.lock();

        try {
            return busy == 0 && lockedOwner == null;
        } finally {
            sync.unlock();
        }
    }

    public void migrateLock(LockIdentity lock) {
        sync.lock();

        try {
            if (lockedOwner == null) {
                lockedOwner = new LockHolder(lock.node(), lock.lockId(), lock.threadId());

                if (DEBUG) {
                    log.debug("Migrated lock [lock={}]", lockedOwner);
                }
            } else if (!lockedOwner.isSameLock(lock)) {
                throw new IllegalStateException("Attempt to supersede lock during migration [name=" + name + ", existing=" + lockedOwner
                    + ", migrating=" + lock + ']');
            }
        } finally {
            sync.unlock();
        }
    }

    public boolean processLock(Message<LockProtocol> msg) {
        assert msg != null : "Message is null.";

        if (DEBUG) {
            log.debug("Got lock request [from={}, request={}]", msg.endpoint(), msg);
        }

        LockRequest request = msg.payload(LockRequest.class);

        sync.lock();

        try {
            if (lockedOwner == null) {
                LockQueueEntry newEntry = new LockQueueEntry(msg, request, null);

                acquireLock(newEntry);
            } else if (lockedOwner.isSameLock(request)) {
                if (DEBUG) {
                    log.debug("Requester is already the lock owner [lock={}]", lockedOwner);
                }

                reply(msg, newResponse(LockResponse.Status.OK));
            } else if (tryReplaceInLockQueue(request, msg)) {
                // Successfully replaced an existing queue entry.
                if (msg.isSubscription()) {
                    replyPartial(msg, newResponse(LockResponse.Status.LOCK_OWNER_CHANGE));
                }
            } else {
                addToLockQueue(msg, request);
            }

            return lockedOwner != null;
        } finally {
            sync.unlock();
        }
    }

    public boolean processUnlock(Message<LockProtocol> msg) {
        assert msg != null : "Message is null.";

        if (DEBUG) {
            log.debug("Got unlock request [from={}, request={}]", msg.endpoint(), msg);
        }

        UnlockRequest request = msg.payload(UnlockRequest.class);

        sync.lock();

        try {
            if (lockedOwner != null) {
                if (lockedOwner.isSameLock(request)) {
                    if (DEBUG) {
                        log.debug("Unlocked [request={}]", msg);
                    }

                    lockedOwner = null;

                    processQueue();
                } else if (!queue.isEmpty()) {
                    for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                        LockQueueEntry oldEntry = it.next();

                        if (oldEntry.request().isSameLock(request)) {
                            it.remove();

                            oldEntry.cancelTimeout();

                            if (DEBUG) {
                                log.debug("Removed from lock queue [entry={}]", oldEntry);
                            }

                            break;
                        }
                    }
                }
            }

            reply(msg, new UnlockResponse(UnlockResponse.Status.OK));

            return lockedOwner != null;
        } finally {
            sync.unlock();
        }
    }

    public void dispose() {
        sync.lock();

        try {
            if (DEBUG) {
                log.debug("Disposing [owner={}, queue-size={}]", lockedOwner, queue.size());
            }

            lockedOwner = null;

            while (!queue.isEmpty()) {
                LockQueueEntry entry = queue.pollFirst();

                if (entry != null) {
                    entry.cancelTimeout();

                    if (DEBUG) {
                        log.debug("Disposed lock queue entry [entry={}]", entry);
                    }

                    reply(entry.message(), newResponse(LockResponse.Status.RETRY));
                }
            }
        } finally {
            sync.unlock();
        }
    }

    public void update(Set<ClusterNodeId> liveNodes) {
        sync.lock();

        try {
            if (!queue.isEmpty()) {
                if (TRACE) {
                    log.trace("Updating live nodes [nodes={}]", liveNodes);
                }

                for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                    LockQueueEntry entry = it.next();

                    LockRequest request = entry.request();

                    if (!liveNodes.contains(request.node())) {
                        entry.cancelTimeout();

                        it.remove();

                        if (DEBUG) {
                            log.debug("Removed lock queue entry of a dead node [entry={}]", entry);
                        }
                    }
                }
            }

            if (lockedOwner != null && !liveNodes.contains(lockedOwner.node())) {
                lockedOwner = null;

                processQueue();
            }
        } finally {
            sync.unlock();
        }
    }

    public void processLockOwnerQuery(Message<LockProtocol> msg) {
        sync.lock();

        try {
            if (lockedOwner == null) {
                reply(msg, new LockOwnerResponse(0, null, LockOwnerResponse.Status.OK));
            } else {
                reply(msg, new LockOwnerResponse(lockedOwner.threadId(), lockedOwner.node(), LockOwnerResponse.Status.OK));
            }
        } finally {
            sync.unlock();
        }
    }

    private boolean tryReplaceInLockQueue(LockRequest req, Message<LockProtocol> msg) {
        assert sync.isHeldByCurrentThread() : "Thread must hold lock.";

        if (!queue.isEmpty()) {
            for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                LockQueueEntry oldEntry = it.next();

                LockRequest oldRequest = oldEntry.request();

                if (oldRequest.isSameLock(req)) {
                    // Remove old entry from the queue.
                    it.remove();

                    LockQueueEntry newEntry = new LockQueueEntry(msg, req, oldEntry.timeoutFuture());

                    // Add new entry to end of the queue.
                    // Note that it breaks the order of locking queue,
                    // but it is ok since we don't have 'fair' locking guarantees anyway.
                    queue.addLast(newEntry);

                    if (DEBUG) {
                        log.debug("Replaced lock queue entry [old={}, new={}]", oldEntry, newEntry);
                    }

                    return true;
                }
            }
        }

        return false;
    }

    private void addToLockQueue(Message<LockProtocol> msg, LockRequest request) {
        assert sync.isHeldByCurrentThread() : "Thread must hold lock.";

        if (request.timeout() == DefaultLockRegion.TIMEOUT_IMMEDIATE) {
            if (DEBUG) {
                log.debug("Rejecting lock request with immediate timeout [request={}]", request);
            }

            // Immediately expire.
            reply(msg, newResponse(LockResponse.Status.LOCK_BUSY));
        } else {
            LockQueueEntry newEntry;

            if (request.timeout() == DefaultLockRegion.TIMEOUT_UNBOUND) {
                // Timeout not specified.
                newEntry = new LockQueueEntry(msg, request, null);
            } else {
                // Register timeout handler.
                ScheduledFuture<?> timeoutFuture = scheduler.schedule(() -> {
                    sync.lock();

                    try {
                        for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                            LockQueueEntry entry = it.next();

                            if (request.isSameLock(entry.request()) && entry.timeoutFuture() != null) {
                                if (entry.message().mustReply()) {
                                    reply(entry.message(), newResponse(LockResponse.Status.LOCK_TIMEOUT));
                                }

                                it.remove();

                                break;
                            }
                        }
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while processing lock timeout [request={}]", request, e);
                    } finally {
                        sync.unlock();
                    }
                }, request.timeout(), TimeUnit.NANOSECONDS);

                newEntry = new LockQueueEntry(msg, request, timeoutFuture);
            }

            queue.add(newEntry);

            if (DEBUG) {
                log.debug("Added lock request to the queue [entry={}]", newEntry);
            }

            // Notify about the current lock owner.
            if (newEntry.message().isSubscription()) {
                replyPartial(newEntry.message(), newResponse(LockResponse.Status.LOCK_OWNER_CHANGE));
            }
        }
    }

    // Package level for testing purposes.
    List<ClusterNodeId> enqueuedLocks() {
        sync.lock();

        try {
            return queue.stream().map(e -> e.request().node()).collect(toList());
        } finally {
            sync.unlock();
        }
    }

    private void acquireLock(LockQueueEntry entry) {
        assert lockedOwner == null : "Lock is already held " + lockedOwner;

        entry.cancelTimeout();

        LockRequest request = entry.request();

        lockedOwner = new LockHolder(request.node(), request.lockId(), request.threadId());

        if (DEBUG) {
            log.debug("Locked [new-owner={}]", entry);
        }

        reply(entry.message(), newResponse(LockResponse.Status.OK));

        queue.stream()
            .filter(e -> e.message().isSubscription())
            .forEach(other -> {
                if (DEBUG) {
                    log.debug("Notifying queue entry on lock owner change [entry={}]", other);
                }

                replyPartial(other.message(), newResponse(LockResponse.Status.LOCK_OWNER_CHANGE));
            });
    }

    private void processQueue() {
        assert sync.isHeldByCurrentThread() : "Thread must hold lock.";

        LockQueueEntry entry = queue.pollFirst();

        if (entry != null) {
            acquireLock(entry);
        }
    }

    private void reply(Message<LockProtocol> msg, LockProtocol reply) {
        msg.reply(reply, err -> {
            if (err == null) {
                if (DEBUG) {
                    log.debug("Sent lock response [to={}, response={}, request={}]", msg.endpoint(), reply, msg);
                }
            } else {
                if (DEBUG) {
                    log.debug("Failed to send lock response [to={}, cause={}, response={}, request={}]",
                        msg.endpoint(), err.toString(), reply, msg);
                }
            }
        });
    }

    private void replyPartial(Message<LockProtocol> msg, LockProtocol reply) {
        msg.partialReply(reply, err -> {
            if (err == null) {
                if (DEBUG) {
                    log.debug("Sent partial lock response [to={}, response={}, request={}]", msg.endpoint(), reply, msg);
                }
            } else {
                if (DEBUG) {
                    log.debug("Failed to send partial lock response [to={}, cause={}, response={}, request={}]",
                        msg.endpoint(), err.toString(), reply, msg);
                }
            }
        });
    }

    private LockResponse newResponse(LockResponse.Status status) {
        assert sync.isHeldByCurrentThread() : "Thread must hold lock.";

        if (lockedOwner == null) {
            return new LockResponse(status, null, 0);
        } else {
            return new LockResponse(status, lockedOwner.node(), lockedOwner.threadId());
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
