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

import io.hekate.network.NetworkEndpoint;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class ReceiveBackPressure {
    private final int loMark;

    private final int hiMark;

    private final AtomicInteger queueSize = new AtomicInteger();

    @ToStringIgnore
    private final IdentityHashMap<NetworkEndpoint<?>, Void> paused = new IdentityHashMap<>();

    @ToStringIgnore
    private final Object mux = new Object();

    public ReceiveBackPressure(int loMark, int hiMark) {
        assert hiMark > 0 : "High watermark must be above zero.";
        assert loMark < hiMark : "Low watermark must less than high watermark [low=" + loMark + ", high=" + hiMark + ']';

        this.loMark = loMark;
        this.hiMark = hiMark;
    }

    public void onEnqueue(NetworkEndpoint<?> endpoint) {
        assert endpoint != null : "Endpoint is  null.";

        int size = queueSize.incrementAndGet();

        if (size >= hiMark) {
            synchronized (mux) {
                // Double check  queue size.
                if (queueSize.get() >= hiMark) {
                    // Pause receiving.
                    paused.put(endpoint, null);

                    endpoint.pauseReceiving(null);
                }
            }
        }
    }

    public void onDequeue() {
        int size = queueSize.decrementAndGet();

        if (size == loMark) { // <-- Strict equality to make sure that only a single thread will resume all receivers.
            synchronized (mux) {
                if (!paused.isEmpty()) {
                    paused.keySet().forEach(endpoint ->
                        endpoint.resumeReceiving(null)
                    );

                    paused.clear();
                }
            }
        }
    }

    public int getQueueSize() {
        return queueSize.get();
    }

    public int getPausedSize() {
        synchronized (mux) {
            return paused.size();
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
