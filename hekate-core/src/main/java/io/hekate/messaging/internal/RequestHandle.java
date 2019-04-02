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

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

class RequestHandle<T> {
    private final Integer id;

    private final int epoch;

    private final MessageOperationAttempt<T> attempt;

    @ToStringIgnore
    private final RequestRegistry<T> registry;

    public RequestHandle(
        Integer id,
        RequestRegistry<T> registry,
        MessageOperationAttempt<T> attempt,
        int epoch
    ) {
        this.id = id;
        this.registry = registry;
        this.attempt = attempt;
        this.epoch = epoch;
    }

    public Integer id() {
        return id;
    }

    public MessagingWorker worker() {
        return attempt.operation().worker();
    }

    public T message() {
        return attempt.operation().message();
    }

    public int epoch() {
        return epoch;
    }

    public MessageOperationAttempt<T> attempt() {
        return attempt;
    }

    public boolean unregister() {
        return registry.unregister(id);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
