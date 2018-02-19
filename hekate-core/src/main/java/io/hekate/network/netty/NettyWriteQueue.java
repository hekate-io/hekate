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

package io.hekate.network.netty;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

class NettyWriteQueue {
    private static final int MAX_FLUSH_BATCH_SIZE = 64;

    private final Queue<DeferredMessage> queue = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean flushScheduled = new AtomicBoolean();

    private final Runnable flushTask;

    private volatile boolean writable;

    public NettyWriteQueue() {
        this(true, null);
    }

    public NettyWriteQueue(boolean writable, NettySpy spy) {
        this.writable = writable;

        flushTask = () -> {
            flushScheduled.set(false);

            DeferredMessage lastNonFlushed = null;

            int cnt = 0;

            for (DeferredMessage msg = queue.poll(); msg != null; msg = queue.poll()) {
                try {
                    if (spy != null) {
                        spy.onBeforeFlush(msg.source());
                    }

                    msg.channel().write(msg, msg.promise());
                } catch (Throwable e) {
                    msg.promise().tryFailure(e);
                }

                lastNonFlushed = msg;

                cnt++;

                if (cnt == MAX_FLUSH_BATCH_SIZE) {
                    lastNonFlushed.channel().flush();

                    lastNonFlushed = null;
                    cnt = 0;
                }
            }

            if (lastNonFlushed != null) {
                lastNonFlushed.channel().flush();
            }
        };
    }

    public void enqueue(DeferredMessage msg, Executor executor) {
        queue.add(msg);

        if (writable) {
            flush(executor);
        }
    }

    public void enableWrites(Executor executor) {
        writable = true;

        flush(executor);
    }

    private void flush(Executor executor) {
        // Check if flush operation is not scheduled yet.
        if (flushScheduled.compareAndSet(false, true)) {
            executor.execute(flushTask);
        }
    }
}
