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

package io.hekate.network.netty;

import io.netty.util.ReferenceCountUtil;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class NettyWriteQueue {
    private static final int MAX_FLUSH_BATCH_SIZE = 64;

    private static final int WRITABLE_OFF = 0;

    private static final int WRITABLE_ON = 1;

    private static final AtomicIntegerFieldUpdater<NettyWriteQueue> WRITABLE_UPDATER = newUpdater(NettyWriteQueue.class, "writable");

    private final Queue<DeferredMessage> queue = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean flushScheduled = new AtomicBoolean();

    private final Runnable flushTask;

    private volatile int writable;

    private volatile Throwable alwaysFails;

    public NettyWriteQueue() {
        this(true, null);
    }

    public NettyWriteQueue(boolean writable, NettySpy spy) {
        this.writable = writable ? WRITABLE_ON : WRITABLE_OFF;

        flushTask = () -> {
            flushScheduled.set(false);

            DeferredMessage lastNonFlushed = null;

            int cnt = 0;

            for (DeferredMessage msg = queue.poll(); msg != null; msg = queue.poll()) {
                Throwable err = this.alwaysFails;

                if (err == null && spy != null) {
                    try {
                        spy.onBeforeFlush(msg.source());
                    } catch (Throwable t) {
                        err = t;
                    }
                }

                if (err == null) {
                    msg.channel().write(msg, msg.promise());

                    lastNonFlushed = msg;

                    cnt++;

                    if (cnt == MAX_FLUSH_BATCH_SIZE) {
                        lastNonFlushed.channel().flush();

                        lastNonFlushed = null;
                        cnt = 0;
                    }
                } else if (msg.promise().tryFailure(err)) {
                    if (msg.isPreEncoded()) {
                        ReferenceCountUtil.release(msg);
                    }
                }
            }

            if (lastNonFlushed != null) {
                lastNonFlushed.channel().flush();
            }
        };
    }

    public void enqueue(DeferredMessage msg, Executor executor) {
        queue.add(msg);

        if (writable == WRITABLE_ON) {
            flush(executor);
        }
    }

    public void enableWrites(Executor executor) {
        if (WRITABLE_UPDATER.compareAndSet(this, WRITABLE_OFF, WRITABLE_ON)) {
            flush(executor);
        }
    }

    public void dispose(Throwable err, Executor executor) {
        alwaysFails = err;

        // Ensure that all pending writes will be processed.
        enableWrites(executor);
    }

    private void flush(Executor executor) {
        // Check if flush operation is not scheduled yet.
        if (flushScheduled.compareAndSet(false, true)) {
            executor.execute(flushTask);
        }
    }
}
