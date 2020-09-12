/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.core.internal;

import io.hekate.core.Hekate;
import io.hekate.core.Hekate.LifecycleListener;
import io.hekate.core.InitializationFuture;
import io.hekate.core.JoinFuture;
import io.hekate.core.LeaveFuture;
import io.hekate.core.TerminateFuture;
import io.hekate.core.internal.util.ArgAssert;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HekateLifecycle {
    private static final Logger log = LoggerFactory.getLogger(Hekate.class);

    private final HekateNode node;

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<>();

    private final ThreadLocal<Boolean> notifying = ThreadLocal.withInitial(() -> false);

    // We are using atomic here only because of the simpler compareAndSet operations, not for concurrency control.
    private final AtomicReference<Boolean> rejoinScheduled = new AtomicReference<>();

    private InitializationFuture initFuture = new InitializationFuture();

    private JoinFuture joinFuture = new JoinFuture();

    private LeaveFuture leaveFuture = new LeaveFuture();

    private TerminateFuture terminateFuture = new TerminateFuture();

    public HekateLifecycle(HekateNode node) {
        this.node = node;
    }

    public InitializationFuture initFuture() {
        return initFuture;
    }

    public JoinFuture joinFuture() {
        return joinFuture;
    }

    public LeaveFuture leaveFuture() {
        return leaveFuture;
    }

    public TerminateFuture terminateFuture() {
        return terminateFuture;
    }

    public boolean isRejoinScheduled() {
        Boolean scheduled = rejoinScheduled.get();

        return scheduled != null && scheduled;
    }

    public void scheduleRejoin() {
        // Enable only if not forcefully disabled yet.
        rejoinScheduled.compareAndSet(null, true);
    }

    public void cancelRejoin() {
        // Cancel only if rejoining is already scheduled.
        rejoinScheduled.compareAndSet(true, false);
    }

    public Runnable terminate(Throwable cause) {
        boolean rejoin = isRejoinScheduled();

        // Cleanup the flag;
        // otherwise, it will affect subsequent lifecycle events.
        rejoinScheduled.set(null);

        Optional<InitializationFuture> oldInit;
        Optional<JoinFuture> oldJoin;
        Optional<LeaveFuture> oldLeave;
        Optional<TerminateFuture> oldTerm;

        if (!rejoin || initFuture.isDone()) {
            oldInit = Optional.of(initFuture);

            initFuture = new InitializationFuture();
        } else {
            oldInit = Optional.empty();
        }

        if (!rejoin || joinFuture.isDone()) {
            oldJoin = Optional.of(joinFuture);

            joinFuture = new JoinFuture();
        } else {
            oldJoin = Optional.empty();
        }

        if (!rejoin || leaveFuture.isDone()) {
            oldLeave = Optional.of(leaveFuture);

            leaveFuture = new LeaveFuture();
        } else {
            oldLeave = Optional.empty();
        }

        if (!rejoin || terminateFuture.isDone()) {
            oldTerm = Optional.of(terminateFuture);

            terminateFuture = new TerminateFuture();
        } else {
            oldTerm = Optional.empty();
        }

        return () -> {
            if (cause == null) {
                oldInit.ifPresent(f -> f.complete(node));
                oldJoin.ifPresent(f -> f.complete(node));
            } else {
                oldInit.ifPresent(f -> f.completeExceptionally(cause));
                oldJoin.ifPresent(f -> f.completeExceptionally(cause));
            }

            oldLeave.ifPresent(f -> f.complete(node));
            oldTerm.ifPresent(f -> f.complete(node));
        };
    }

    public void checkReentrancy() {
        if (notifying.get()) {
            throw new IllegalStateException("Can't change state inside of a " + LifecycleListener.class.getName());
        }
    }

    public void add(LifecycleListener listener) {
        ArgAssert.notNull(listener, "Listener");

        if (log.isDebugEnabled()) {
            log.debug("Adding lifecycle listener [listener={}]", listener);
        }

        listeners.add(listener);
    }

    public boolean remove(LifecycleListener listener) {
        if (listener != null && listeners.remove(listener)) {
            if (log.isDebugEnabled()) {
                log.debug("Removed lifecycle listener [listener={}]", listener);
            }

            return true;
        }

        return false;
    }

    public void notifyStateChange() {
        notifying.set(true);

        try {
            Hekate.State state = node.state();

            for (LifecycleListener listener : listeners) {
                try {
                    listener.onStateChanged(node);
                } catch (Throwable e) {
                    log.error("Failed to notify listener on state change [state={}, listener={}]", state, listener, e);
                }
            }
        } finally {
            notifying.set(false);
        }
    }
}
