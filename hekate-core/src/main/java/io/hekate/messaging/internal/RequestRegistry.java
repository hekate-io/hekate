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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

class RequestRegistry<T> {
    private static final int REQUEST_MAP_INIT_CAPACITY = 128;

    private final Map<Integer, RequestHandle<T>> requests = new ConcurrentHashMap<>(REQUEST_MAP_INIT_CAPACITY);

    private final AtomicInteger idGen = new AtomicInteger();

    private final MessagingMetrics metrics;

    public RequestRegistry(MessagingMetrics metrics) {
        this.metrics = metrics;
    }

    public RequestHandle<T> register(int epoch, MessageOperationAttempt<T> attempt) {
        while (true) {
            Integer id = idGen.incrementAndGet();

            RequestHandle<T> req = new RequestHandle<>(id, this, attempt, epoch);

            // Do not overwrite very very very old requests.
            if (requests.putIfAbsent(id, req) == null) {
                metrics.onPendingRequestAdded();

                return req;
            }
        }
    }

    public RequestHandle<T> get(Integer id) {
        return requests.get(id);
    }

    public List<RequestHandle<T>> unregisterEpoch(int epoch) {
        List<RequestHandle<T>> removed = new ArrayList<>(requests.size());

        for (RequestHandle<T> req : requests.values()) {
            if (req.epoch() == epoch) {
                if (req.unregister()) {
                    removed.add(req);
                }
            }
        }

        return removed;
    }

    public boolean isEmpty() {
        return requests.isEmpty();
    }

    public boolean unregister(Integer id) {
        RequestHandle<T> req = requests.remove(id);

        if (req != null) {
            metrics.onPendingRequestsRemoved(1);

            return true;
        }

        return false;
    }
}
