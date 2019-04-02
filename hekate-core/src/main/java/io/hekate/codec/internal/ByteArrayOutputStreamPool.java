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

package io.hekate.codec.internal;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

class ByteArrayOutputStreamPool {
    /** Initial size of a pooled {@link ByteArrayOutputStream}. */
    private static final int INITIAL_BUFFER_SIZE = 512;

    /** Thread-local pool of {@link ByteArrayOutputStream}s. */
    // Non-static because of every instance can have its own {@link #maxBufferSize}).
    private final ThreadLocal<ArrayList<ByteArrayOutputStream>> threadLocalPool = ThreadLocal.withInitial(() ->
        new ArrayList<>(1)
    );

    /** If {@link ByteArrayOutputStream#size()} is greater than this size then it will NOT be reused. */
    private final int maxBufferSize;

    public ByteArrayOutputStreamPool(int maxBufferSize) {
        this.maxBufferSize = maxBufferSize;
    }

    public ByteArrayOutputStream acquire() {
        ByteArrayOutputStream buf;

        ArrayList<ByteArrayOutputStream> buffers = threadLocalPool.get();

        if (buffers.isEmpty()) {
            buf = new ByteArrayOutputStream(INITIAL_BUFFER_SIZE);
        } else {
            // Remove from the tail (for ArrayList it is a cheap operation).
            buf = buffers.remove(buffers.size() - 1);
        }

        return buf;
    }

    public boolean recycle(ByteArrayOutputStream buf) {
        if (buf.size() <= maxBufferSize) {
            // Clear the buffer.
            buf.reset();

            // Add the buffer back to the pool.
            return threadLocalPool.get().add(buf);
        } else {
            return false;
        }
    }
}
