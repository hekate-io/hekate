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

package io.hekate.messaging.internal;

import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.atomic.AtomicInteger;

class SendBackPressure {
    private final int loMark;

    private final int hiMark;

    private final MessagingOverflowPolicy policy;

    private final AtomicInteger queueSize = new AtomicInteger();

    @ToStringIgnore
    private final Object mux = new Object();

    @ToStringIgnore
    private boolean stopped;

    public SendBackPressure(int loMark, int hiMark, MessagingOverflowPolicy policy) {
        assert hiMark > 0 : "High watermark must be above zero.";
        assert loMark < hiMark : "Low watermark must less than high watermark [low=" + loMark + ", high=" + hiMark + ']';
        assert policy != null : "Policy must be not null.";
        assert policy != MessagingOverflowPolicy.IGNORE : "Unexpected overflow policy: " + policy;

        this.loMark = loMark;
        this.hiMark = hiMark;
        this.policy = policy;
    }

    public void onEnqueueIgnorePolicy() {
        queueSize.incrementAndGet();
    }

    public Exception onEnqueue() {
        int size = queueSize.incrementAndGet();

        if (size > hiMark) {
            Exception rejected = null;

            synchronized (mux) {
                // Double check queue size before applying policy.
                if (queueSize.get() > hiMark) {
                    switch (policy) {
                        case BLOCK: {
                            rejected = block();

                            break;
                        }
                        case BLOCK_UNINTERRUPTEDLY: {
                            blockUninterruptedly();

                            break;
                        }
                        case FAIL: {
                            rejected = new MessageQueueOverflowException("Send queue overflow "
                                + "[queue-size=" + queueSize + ", low-watermark=" + loMark + ", high-watermark=" + hiMark + ']');

                            break;
                        }
                        case IGNORE:
                        default: {
                            throw new IllegalArgumentException("Unexpected overflow policy: " + policy);
                        }
                    }
                }
            }

            return rejected;
        }

        return null;
    }

    public void onDequeue() {
        int size = queueSize.decrementAndGet();

        if (size == loMark) { // <-- Strict equality to make sure that only a single thread will notify others.
            synchronized (mux) {
                mux.notifyAll();
            }
        }
    }

    public void terminate() {
        synchronized (mux) {
            stopped = true;

            mux.notifyAll();
        }
    }

    public int getQueueSize() {
        return queueSize.get();
    }

    private Exception block() {
        assert Thread.holdsLock(mux) : "Thread must hold lock on mutex.";

        while (!stopped && queueSize.get() > loMark) {
            try {
                mux.wait();
            } catch (InterruptedException err) {
                return err;
            }
        }

        return null;
    }

    private void blockUninterruptedly() {
        assert Thread.holdsLock(mux) : "Thread must hold lock on mutex.";

        boolean interrupted = false;

        while (!stopped && queueSize.get() > loMark) {
            try {
                mux.wait();
            } catch (InterruptedException err) {
                interrupted = true;
            }
        }

        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
