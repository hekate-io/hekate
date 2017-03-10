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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.lock.internal.LockProtocol.LockOwnerResponse;
import io.hekate.lock.internal.LockProtocol.LockRequest;
import io.hekate.lock.internal.LockProtocol.LockResponse;
import io.hekate.lock.internal.LockProtocol.UnlockRequest;
import io.hekate.lock.internal.LockProtocol.UnlockResponse;
import io.hekate.messaging.Message;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
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
        public ClusterNodeId getNode() {
            return node;
        }

        @Override
        public long getThreadId() {
            return threadId;
        }

        @Override
        public long getLockId() {
            return lockId;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static class LockQueueEntry {
        private final LockRequest request;

        @ToStringIgnore
        private final Message<LockProtocol> message;

        @ToStringIgnore
        private final ScheduledFuture<?> timeoutFuture;

        public LockQueueEntry(LockRequest request, Message<LockProtocol> message, ScheduledFuture<?> timeoutFuture) {
            this.message = message;
            this.request = request;
            this.timeoutFuture = timeoutFuture;
        }

        public Message<LockProtocol> getMessage() {
            return message;
        }

        public LockRequest getRequest() {
            return request;
        }

        public ScheduledFuture<?> getTimeoutFuture() {
            return timeoutFuture;
        }

        public boolean isWithFeedback() {
            return request.isWithFeedback();
        }

        public void cancelTimeout() {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(LockControllerServer.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final ReentrantLock sync = new ReentrantLock();

    private final ScheduledExecutorService scheduler;

    private final LinkedList<LockQueueEntry> queue = new LinkedList<>();

    private final String name;

    private LockHolder lockedBy;

    private int busy;

    public LockControllerServer(String name, ScheduledExecutorService scheduler) {
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
            return busy == 0 && lockedBy == null;
        } finally {
            sync.unlock();
        }
    }

    public void migrateLock(LockIdentity lock) {
        sync.lock();

        try {
            if (lockedBy == null) {
                lockedBy = new LockHolder(lock.getNode(), lock.getLockId(), lock.getThreadId());

                if (DEBUG) {
                    log.debug("Migrated lock [lock={}]", lockedBy);
                }
            } else if (!lockedBy.isSameLock(lock)) {
                throw new IllegalStateException("Attempt to supersede lock during migration [name=" + name + ", existing=" + lockedBy
                    + ", migrating=" + lock + ']');
            }
        } finally {
            sync.unlock();
        }
    }

    public boolean processLock(Message<LockProtocol> msg) {
        assert msg != null : "Message is null.";

        LockRequest request = msg.get(LockRequest.class);

        sync.lock();

        try {
            if (lockedBy == null) {
                LockQueueEntry newEntry = new LockQueueEntry(request, msg, null);

                acquireLock(newEntry);
            } else if (lockedBy.isSameLock(request)) {
                if (DEBUG) {
                    log.debug("Requester is already the lock owner [lock={}]", lockedBy);
                }

                reply(msg, newResponse(LockResponse.Status.OK));
            } else {
                boolean replaced = false;

                if (!queue.isEmpty()) {
                    for (ListIterator<LockQueueEntry> it = queue.listIterator(); it.hasNext(); ) {
                        LockQueueEntry oldEntry = it.next();

                        LockRequest oldRequest = oldEntry.getRequest();

                        if (oldRequest.isSameLock(request)) {
                            LockQueueEntry newEntry = new LockQueueEntry(request, msg, oldEntry.getTimeoutFuture());

                            it.set(newEntry);

                            if (DEBUG) {
                                log.debug("Replaced lock request in the queue [old={}, new={}, queue={}]", oldEntry, newEntry, queue);
                            }

                            reply(oldEntry.getMessage(), newResponse(LockResponse.Status.REPLACED));

                            replaced = true;

                            if (request.isWithFeedback()) {
                                replyPartial(newEntry.getMessage(), newResponse(LockResponse.Status.LOCK_INFO));
                            }

                            break;
                        }
                    }
                }

                if (!replaced) {
                    if (request.getTimeout() == DefaultLockRegion.TIMEOUT_IMMEDIATE) {
                        if (DEBUG) {
                            log.debug("Rejecting lock request with immediate timeout [request={}]", request);
                        }

                        // Immediately expire.
                        reply(msg, newResponse(LockResponse.Status.BUSY));
                    } else if (request.getTimeout() == DefaultLockRegion.TIMEOUT_UNBOUND) {
                        // Enqueue without any timeouts.
                        LockQueueEntry entry = new LockQueueEntry(request, msg, null);

                        queue.add(entry);

                        if (DEBUG) {
                            log.debug("Added lock request to the locking queue [new={}, queue={}]", entry, queue);
                        }

                        if (request.isWithFeedback()) {
                            replyPartial(entry.getMessage(), newResponse(LockResponse.Status.LOCK_INFO));
                        }
                    } else {
                        // Register timeout handler.
                        ScheduledFuture<?> future = scheduler.schedule(() -> {
                            sync.lock();

                            try {
                                for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                                    LockQueueEntry entry = it.next();

                                    LockRequest entryRequest = entry.getRequest();

                                    if (request.isSameLock(entryRequest) && entry.getTimeoutFuture() != null) {
                                        if (entry.getMessage().mustReply()) {
                                            reply(entry.getMessage(), newResponse(LockResponse.Status.TIMEOUT));
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
                        }, request.getTimeout(), TimeUnit.NANOSECONDS);

                        LockQueueEntry entry = new LockQueueEntry(request, msg, future);

                        queue.add(entry);

                        if (DEBUG) {
                            log.debug("Added lock request with timeout to the locking queue [new={}, queue={}]", entry, queue);
                        }

                        if (entry.isWithFeedback()) {
                            replyPartial(entry.getMessage(), newResponse(LockResponse.Status.LOCK_INFO));
                        }
                    }
                }
            }

            return lockedBy != null;
        } finally {
            sync.unlock();
        }
    }

    public boolean processUnlock(Message<LockProtocol> msg) {
        assert msg != null : "Message is null.";

        UnlockRequest request = msg.get(UnlockRequest.class);

        sync.lock();

        try {
            if (lockedBy != null) {
                if (lockedBy.isSameLock(request)) {
                    if (DEBUG) {
                        log.debug("Unlocked [request={}, queue={}]", request, queue);
                    }

                    lockedBy = null;

                    processQueue();
                } else if (!queue.isEmpty()) {
                    for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                        LockQueueEntry oldEntry = it.next();

                        if (oldEntry.getRequest().isSameLock(request)) {
                            it.remove();

                            oldEntry.cancelTimeout();

                            if (DEBUG) {
                                log.debug("Removed from lock queue [entry={}, queue={}]", oldEntry, queue);
                            }

                            reply(oldEntry.getMessage(), newResponse(LockResponse.Status.REPLACED));

                            break;
                        }
                    }
                }
            }

            reply(msg, new UnlockResponse(UnlockResponse.Status.OK));

            return lockedBy != null;
        } finally {
            sync.unlock();
        }
    }

    public void dispose() {
        sync.lock();

        try {
            lockedBy = null;

            while (!queue.isEmpty()) {
                LockQueueEntry entry = queue.pollFirst();

                entry.cancelTimeout();

                if (DEBUG) {
                    log.debug("Disposed lock queue entry [entry={}]", entry);
                }

                reply(entry.getMessage(), newResponse(LockResponse.Status.RETRY));
            }
        } finally {
            sync.unlock();
        }
    }

    public void update(Set<ClusterNodeId> liveNodes) {
        sync.lock();

        try {
            if (DEBUG) {
                log.debug("Updating live nodes [nodes={}]", liveNodes);
            }

            for (Iterator<LockQueueEntry> it = queue.iterator(); it.hasNext(); ) {
                LockQueueEntry entry = it.next();

                LockRequest request = entry.getRequest();

                if (!liveNodes.contains(request.getNode())) {
                    entry.cancelTimeout();

                    it.remove();

                    if (DEBUG) {
                        log.debug("Removed lock queue entry of a dead node [entry={}]", entry);
                    }
                }
            }

            if (lockedBy != null && !liveNodes.contains(lockedBy.getNode())) {
                lockedBy = null;

                processQueue();
            }
        } finally {
            sync.unlock();
        }
    }

    public void processOwnerQuery(Message<LockProtocol> msg) {
        sync.lock();

        try {
            if (lockedBy == null) {
                reply(msg, new LockOwnerResponse(0, null, LockOwnerResponse.Status.OK));
            } else {
                reply(msg, new LockOwnerResponse(lockedBy.getThreadId(), lockedBy.getNode(), LockOwnerResponse.Status.OK));
            }
        } finally {
            sync.unlock();
        }
    }

    // Package level for testing purposes.
    List<ClusterNodeId> getQueuedLocks() {
        sync.lock();

        try {
            return queue.stream().map(e -> e.getRequest().getNode()).collect(toList());
        } finally {
            sync.unlock();
        }
    }

    private void acquireLock(LockQueueEntry entry) {
        assert lockedBy == null : "Lock is already held " + lockedBy;

        entry.cancelTimeout();

        LockRequest request = entry.getRequest();

        lockedBy = new LockHolder(request.getNode(), request.getLockId(), request.getThreadId());

        if (DEBUG) {
            log.debug("Locked [new-owner={}, queue={}]", lockedBy, queue);
        }

        reply(entry.getMessage(), newResponse(LockResponse.Status.OK));

        queue.stream()
            .filter(LockQueueEntry::isWithFeedback)
            .forEach(other -> {
                if (DEBUG) {
                    log.debug("Updating lock owner info [queue-entry={}]", other);
                }

                replyPartial(other.getMessage(), newResponse(LockResponse.Status.LOCK_INFO));
            });
    }

    private void processQueue() {
        LockQueueEntry entry = queue.pollFirst();

        if (entry != null) {
            acquireLock(entry);
        }
    }

    private void reply(Message<LockProtocol> msg, LockProtocol reply) {
        msg.reply(reply, err -> {
            if (err == null) {
                if (DEBUG) {
                    log.debug("Successfully sent lock response [response={}, request={}]", reply, msg.get());
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Failed to send lock response [cause={}, response={}, request={}]", err.toString(), reply, msg.get());
                }
            }
        });
    }

    private void replyPartial(Message<LockProtocol> msg, LockProtocol reply) {
        msg.replyPartial(reply, err -> {
            if (err == null) {
                if (DEBUG) {
                    log.debug("Successfully sent partial lock response [response={}, request={}]", reply, msg.get());
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Failed to send partial lock response [cause={}, response={}, request={}]", err.toString(), reply, msg.get());
                }
            }
        });
    }

    private LockResponse newResponse(LockResponse.Status status) {
        assert sync.isHeldByCurrentThread() : "Thread must hold lock.";

        if (lockedBy == null) {
            return new LockResponse(status, null, 0);
        } else {
            return new LockResponse(status, lockedBy.getNode(), lockedBy.getThreadId());
        }
    }
}
