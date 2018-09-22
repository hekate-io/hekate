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

import io.hekate.messaging.intercept.OutboundType;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

class MessageContext<T> {
    interface TimeoutListener {
        void onTimeout();
    }

    private static final AtomicIntegerFieldUpdater<MessageContext> STATE = newUpdater(
        MessageContext.class,
        "state"
    );

    private static final AtomicReferenceFieldUpdater<MessageContext, Future> TIMEOUT_FUTURE = newUpdater(
        MessageContext.class,
        Future.class,
        "timeoutFuture"
    );

    private static final int STATE_PENDING = 0;

    private static final int STATE_RECEIVED = 1;

    private static final int STATE_COMPLETED = 2;

    private static final int STATE_ANY = -1;

    private final int affinity;

    private final Object affinityKey;

    private final OutboundType type;

    private final T message;

    private final MessagingWorker worker;

    private final MessagingOpts<T> opts;

    private final InterceptorManager<T> interceptor;

    private Map<String, Object> attributes;

    private volatile TimeoutListener timeoutListener;

    @SuppressWarnings("unused") // <-- Updated via AtomicReferenceFieldUpdater.
    private volatile Future<?> timeoutFuture;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int state;

    public MessageContext(
        T message,
        int affinity,
        Object affinityKey,
        MessagingWorker worker,
        MessagingOpts<T> opts,
        OutboundType type,
        InterceptorManager<T> interceptor
    ) {
        this.message = message;
        this.worker = worker;
        this.opts = opts;
        this.affinityKey = affinityKey;
        this.affinity = affinity;
        this.type = type;
        this.interceptor = interceptor;
    }

    public boolean hasAffinity() {
        return affinityKey != null;
    }

    public int affinity() {
        return affinity;
    }

    public Object affinityKey() {
        return affinityKey;
    }

    public OutboundType type() {
        return type;
    }

    public T originalMessage() {
        return message;
    }

    public MessagingWorker worker() {
        return worker;
    }

    public MessagingOpts<T> opts() {
        return opts;
    }

    public InterceptorManager<T> intercept() {
        return interceptor;
    }

    public boolean isCompleted() {
        return state == STATE_COMPLETED;
    }

    public boolean complete() {
        boolean completed = doComplete(STATE_ANY);

        if (completed) {
            Future<?> localFuture = this.timeoutFuture;

            if (localFuture != null) {
                localFuture.cancel(false);
            }
        }

        return completed;
    }

    public boolean completeOnTimeout() {
        boolean completed = doComplete(STATE_PENDING);

        if (completed) {
            if (timeoutListener != null) {
                timeoutListener.onTimeout();
            }
        }

        return completed;
    }

    public void setTimeoutListener(TimeoutListener timeoutListener) {
        assert timeoutListener != null : "Timeout listener is null.";
        assert opts.hasTimeout() : "Timeout listener can be set only for time-limited contexts.";

        this.timeoutListener = timeoutListener;

        if (isCompleted()) {
            timeoutListener.onTimeout();
        }
    }

    public void keepAlive() {
        STATE.compareAndSet(this, STATE_PENDING, STATE_RECEIVED);
    }

    public void setTimeoutFuture(Future<?> timeoutFuture) {
        // 1) Try to set as initial timeout future.
        if (!TIMEOUT_FUTURE.compareAndSet(this, null, timeoutFuture)) {
            // 2) This is a refreshed future for streams -> Try to refresh with state checks.
            if (STATE.compareAndSet(this, STATE_RECEIVED, STATE_PENDING)) {
                this.timeoutFuture = timeoutFuture;

                // Double-check that we didn't switch to COMPLETED state while updating the future field.
                if (isCompleted()) {
                    timeoutFuture.cancel(false);
                }
            }
        }
    }

    public Object setAttribute(String name, Object value) {
        if (attributes == null) {
            attributes = new HashMap<>();
        }

        return attributes.put(name, value);
    }

    public Object getAttribute(String name) {
        return attributes != null ? attributes.get(name) : null;
    }

    private boolean doComplete(int expectedState) {
        while (true) {
            int localState = this.state;

            if (expectedState != STATE_ANY && localState != expectedState) {
                // Precondition didn't match.
                return false;
            }

            switch (localState) {
                case STATE_PENDING: {
                    if (STATE.compareAndSet(this, STATE_PENDING, STATE_COMPLETED)) {
                        return true;
                    }

                    break;
                }
                case STATE_RECEIVED: {
                    if (STATE.compareAndSet(this, STATE_RECEIVED, STATE_COMPLETED)) {
                        return true;
                    }

                    break;
                }
                case STATE_COMPLETED: {
                    return false;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected state of messaging context: " + localState);
                }
            }
        }
    }
}
