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

package io.hekate.codec.internal;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.ArrayBlockingQueue;

class ByteArrayOutputStreamPool {
    public static final int INITIAL_BUFFER_SIZE = 1024;

    private final ArrayBlockingQueue<ByteArrayOutputStream> pool;

    private final int maxBufferSize;

    public ByteArrayOutputStreamPool(int maxPoolSize, int maxBufferSize) {
        this.pool = new ArrayBlockingQueue<>(maxPoolSize);

        this.maxBufferSize = maxBufferSize;
    }

    public ByteArrayOutputStream acquire() {
        ByteArrayOutputStream buf = pool.poll();

        if (buf == null) {
            buf = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        } else {
            buf.reset();
        }

        return buf;
    }

    public boolean recycle(ByteArrayOutputStream buf) {
        if (buf.size() <= maxBufferSize) {
            return pool.offer(buf);
        } else {
            return false;
        }
    }
}
