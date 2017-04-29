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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class RequestRegistry<T> {
    private static final int REQUEST_MAP_INIT_CAPACITY = 128;

    private final AtomicInteger idGen = new AtomicInteger();

    private final Map<Integer, RequestHandle<T>> requests = new ConcurrentHashMap<>(REQUEST_MAP_INIT_CAPACITY);

    private final MetricsCallback metrics;

    public RequestRegistry(MetricsCallback metrics) {
        this.metrics = metrics;
    }

    public RequestHandle<T> register(int epoch, MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        while (true) {
            Integer id = idGen.incrementAndGet();

            RequestHandle<T> handle = new RequestHandle<>(id, this, ctx, epoch, callback);

            // Do not overwrite very very very old requests.
            if (requests.putIfAbsent(id, handle) == null) {
                onRequestRegister();

                if (ctx.opts().hasTimeout()) {
                    // Unregister if messaging operation gets timed out.
                    ctx.setTimeoutListener(() ->
                        unregister(id)
                    );
                }

                return handle;
            }
        }
    }

    public RequestHandle<T> get(Integer id) {
        return requests.get(id);
    }

    public List<RequestHandle<T>> unregisterEpoch(int epoch) {
        List<RequestHandle<T>> removed = new ArrayList<>(requests.size());

        for (RequestHandle<T> handle : requests.values()) {
            if (handle.epoch() == epoch) {
                if (handle.unregister()) {
                    removed.add(handle);
                }
            }
        }

        onRequestUnregister(removed.size());

        return removed;
    }

    public boolean isEmpty() {
        return requests.isEmpty();
    }

    public boolean unregister(Integer id) {
        RequestHandle<T> handle = requests.remove(id);

        if (handle != null) {
            onRequestUnregister(1);

            return true;
        }

        return false;
    }

    private void onRequestRegister() {
        if (metrics != null) {
            metrics.onPendingRequestAdded();
        }
    }

    private void onRequestUnregister(int amount) {
        if (metrics != null) {
            metrics.onPendingRequestsRemoved(amount);
        }
    }
}
