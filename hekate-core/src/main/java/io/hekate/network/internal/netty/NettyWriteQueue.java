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

package io.hekate.network.internal.netty;

import io.netty.channel.Channel;
import io.netty.channel.DefaultChannelPromise;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

class NettyWriteQueue {
    static class WritePromise extends DefaultChannelPromise {
        private final Object message;

        public WritePromise(Object message, Channel channel) {
            super(channel);

            this.message = message;
        }

        public Object getMessage() {
            return message;
        }
    }

    private static final int MAX_FLUSH_BATCH_SIZE = 64;

    private final Queue<WritePromise> queue = new ConcurrentLinkedQueue<>();

    private final AtomicBoolean flushScheduled = new AtomicBoolean();

    private final Runnable flushTask = () -> {
        flushScheduled.set(false);

        WritePromise lastNonFlushed = null;

        int cnt = 0;

        for (WritePromise promise = queue.poll(); promise != null; promise = queue.poll()) {
            promise.channel().write(promise.getMessage(), promise);

            lastNonFlushed = promise;

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

    public void enqueue(WritePromise promise, Executor executor) {
        queue.add(promise);

        // Check if flush operation should be enqueued too.
        if (flushScheduled.compareAndSet(false, true)) {
            executor.execute(flushTask);
        }
    }
}
