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

package io.hekate.messaging.internal;

import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageQueueTimeoutException;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class SendPressureGuard {
    private final int loMark;

    private final int hiMark;

    private final MessagingOverflowPolicy policy;

    @ToStringIgnore
    private final AtomicInteger queueSize = new AtomicInteger();

    @ToStringIgnore
    private final AtomicInteger blockedSize = new AtomicInteger();

    @ToStringIgnore
    private final ReentrantLock lock = new ReentrantLock();

    @ToStringIgnore
    private final Condition block = lock.newCondition();

    @ToStringIgnore
    private boolean stopped;

    public SendPressureGuard(int loMark, int hiMark, MessagingOverflowPolicy policy) {
        assert hiMark > 0 : "High watermark must be above zero.";
        assert loMark < hiMark : "Low watermark must less than high watermark [low=" + loMark + ", high=" + hiMark + ']';
        assert policy != null : "Policy must be not null.";
        assert policy != MessagingOverflowPolicy.IGNORE : "Unexpected overflow policy: " + policy;

        this.loMark = loMark;
        this.hiMark = hiMark;
        this.policy = policy;
    }

    public int loMark() {
        return loMark;
    }

    public int hiMark() {
        return hiMark;
    }

    public MessagingOverflowPolicy policy() {
        return policy;
    }

    public void onEnqueueIgnorePolicy() {
        queueSize.incrementAndGet();
    }

    public void onEnqueue() throws InterruptedException, MessageQueueOverflowException {
        try {
            onEnqueue(0, null);
        } catch (MessageQueueTimeoutException e) {
            throw new AssertionError("Unexpected timeout error.", e);
        }
    }

    public long onEnqueue(long timeout, Object msg) throws InterruptedException, MessageQueueOverflowException,
        MessageQueueTimeoutException {
        int size = queueSize.incrementAndGet();

        if (size > hiMark) {
            lock.lock();

            try {
                // Double check queue size before applying policy.
                if (queueSize.get() > hiMark) {
                    return applyPolicy(timeout, msg);
                }
            } catch (InterruptedException | MessageQueueOverflowException | MessageQueueTimeoutException e) {
                // Dequeue on error.
                onDequeue();

                throw e;
            } finally {
                lock.unlock();
            }
        }

        return timeout;
    }

    public void onDequeue() {
        int size = queueSize.decrementAndGet();

        if ((size > 0) && (size - blockedSize.get() == loMark)) { // <-- Strict equality so that only a single thread will notify others.
            lock.lock();

            try {
                block.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }

    public void terminate() {
        lock.lock();

        try {
            stopped = true;

            block.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public int queueSize() {
        return queueSize.get();
    }

    private long applyPolicy(long timeout, Object msg) throws InterruptedException, MessageQueueTimeoutException,
        MessageQueueOverflowException {
        switch (policy) {
            case BLOCK: {
                return block(timeout, msg);
            }
            case BLOCK_UNINTERRUPTEDLY: {
                return blockUninterruptedly(timeout, msg);
            }
            case FAIL: {
                throw new MessageQueueOverflowException("Send queue overflow "
                    + "[queue-size=" + queueSize + ", low-watermark=" + loMark + ", high-watermark=" + hiMark + ']');
            }
            case IGNORE:
            default: {
                throw new IllegalArgumentException("Unexpected overflow policy: " + policy);
            }
        }
    }

    private long block(long timeout, Object msg) throws InterruptedException, MessageQueueTimeoutException {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock.";

        blockedSize.incrementAndGet();

        try {
            long deadline = timeout > 0 ? TimeUnit.MILLISECONDS.toNanos(timeout) : Long.MAX_VALUE;

            while (deadline > 0 && !stopped && queueSize.get() - blockedSize.get() > loMark) {
                deadline = block.awaitNanos(deadline);
            }

            if (timeout > 0) {
                return checkDeadline(TimeUnit.NANOSECONDS.toMillis(deadline), msg);
            } else {
                return timeout;
            }
        } finally {
            blockedSize.decrementAndGet();
        }
    }

    private long blockUninterruptedly(long timeout, Object msg) throws MessageQueueTimeoutException {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock.";

        blockedSize.incrementAndGet();

        try {
            boolean interrupted = false;

            long deadline = timeout > 0 ? TimeUnit.MILLISECONDS.toNanos(timeout) : Long.MAX_VALUE;

            while (deadline > 0 && !stopped && queueSize.get() - blockedSize.get() > loMark) {
                try {
                    deadline = block.awaitNanos(deadline);
                } catch (InterruptedException err) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            if (timeout > 0) {
                return checkDeadline(TimeUnit.NANOSECONDS.toMillis(deadline), msg);
            } else {
                return timeout;
            }
        } finally {
            blockedSize.decrementAndGet();
        }
    }

    private long checkDeadline(long deadline, Object msg) throws MessageQueueTimeoutException {
        if (deadline > 0) {
            return deadline;
        } else {
            throw new MessageQueueTimeoutException("Messaging operation timed out while awaiting on back pressure control queue "
                + "[message=" + msg + ']');
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
