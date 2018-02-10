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

package io.hekate.messaging.internal;

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class RequestHandle<T> {
    private static final AtomicIntegerFieldUpdater<RequestHandle> UNREGISTERED = newUpdater(RequestHandle.class, "unregistered");

    private final Integer id;

    private final int epoch;

    private final MessageRoute<T> route;

    @ToStringIgnore
    private final InternalRequestCallback<T> callback;

    @ToStringIgnore
    private final RequestRegistry<T> registry;

    @ToStringIgnore
    @SuppressWarnings("unused")
    private volatile int unregistered;

    public RequestHandle(
        Integer id,
        RequestRegistry<T> registry,
        MessageRoute<T> route,
        int epoch,
        InternalRequestCallback<T> callback
    ) {
        this.id = id;
        this.registry = registry;
        this.route = route;
        this.epoch = epoch;
        this.callback = callback;
    }

    public Integer id() {
        return id;
    }

    public MessagingWorker worker() {
        return route.ctx().worker();
    }

    public T message() {
        return route.ctx().originalMessage();
    }

    public int epoch() {
        return epoch;
    }

    public InternalRequestCallback<T> callback() {
        return callback;
    }

    public MessageContext<T> context() {
        return route.ctx();
    }

    public MessageRoute<T> route() {
        return route;
    }

    public boolean isRegistered() {
        return unregistered == 0;
    }

    public boolean unregister() {
        if (UNREGISTERED.compareAndSet(this, 0, 1)) {
            registry.unregister(id);

            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
