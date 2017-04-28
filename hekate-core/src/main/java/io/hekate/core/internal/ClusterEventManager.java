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

package io.hekate.core.internal;

import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Utils;
import io.hekate.util.format.ToString;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;

class ClusterEventManager {
    private static class FilteredListener implements ClusterEventListener {
        private final EnumSet<ClusterEventType> eventTypes;

        private final ClusterEventListener delegate;

        public FilteredListener(ClusterEventListener delegate) {
            this(null, delegate);
        }

        public FilteredListener(EnumSet<ClusterEventType> eventTypes, ClusterEventListener delegate) {
            this.eventTypes = eventTypes;
            this.delegate = delegate;
        }

        @Override
        public void onEvent(ClusterEvent event) throws HekateException {
            if (eventTypes == null || eventTypes.contains(event.getType())) {
                if (DEBUG) {
                    log.debug("Notifying listener on event [listener={}, event={}]", delegate, event);
                }

                delegate.onEvent(event);
            } else {
                if (DEBUG) {
                    log.debug("Skipped listener notification since it is not interested in the target event type "
                        + "[listener={}, eventTypes={}, event={}]", delegate, eventTypes, event);
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof FilteredListener)) {
                return false;
            }

            FilteredListener that = (FilteredListener)o;

            return !(delegate != null ? !delegate.equals(that.delegate) : that.delegate != null);
        }

        @Override
        public int hashCode() {
            return delegate != null ? delegate.hashCode() : 0;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(ClusterEventManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final ThreadLocal<Boolean> inAsync = new ThreadLocal<>();

    private final AtomicBoolean joinEventFired = new AtomicBoolean();

    private final List<FilteredListener> listeners = new CopyOnWriteArrayList<>();

    private volatile ExecutorService worker;

    private volatile ClusterTopology lastTopology;

    public CompletableFuture<?> fireAsync(ClusterEvent event) {
        if (DEBUG) {
            log.debug("Scheduled cluster event for asynchronous processing [event={}]", event);
        }

        if (event.getType() == ClusterEventType.JOIN) {
            joinEventFired.set(true);
        } else if (event.getType() == ClusterEventType.LEAVE) {
            joinEventFired.set(false);
        }

        CompletableFuture<?> future = new CompletableFuture<>();

        worker.execute(() -> {
            inAsync.set(true);

            try {
                if (DEBUG) {
                    log.debug("Notifying listeners on cluster event [listeners={}, event={}]", listeners.size(), event);
                }

                lastTopology = event.getTopology();

                for (ClusterEventListener listener : listeners) {
                    try {
                        listener.onEvent(event);
                    } catch (Throwable t) {
                        log.error("Failed to notify cluster event listener [listener={}]", listener, t);
                    }
                }

                try {
                    future.complete(null);
                } catch (Throwable t) {
                    log.error("Failed to notify cluster event processing future [event={}]", event, t);
                }
            } finally {
                inAsync.remove();
            }
        });

        return future;
    }

    public CompletableFuture<?> ensureLeaveEventFired(ClusterTopology topology) {
        // If join event had been fired then we need to fire leave event too.
        if (joinEventFired.compareAndSet(true, false)) {
            ClusterLeaveEvent event = new ClusterLeaveEvent(topology, emptyList(), emptyList());

            return fireAsync(event);
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    public boolean isJoinEventFired() {
        return joinEventFired.get();
    }

    public void addListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        doAddListener(new FilteredListener(listener));
    }

    public void addListener(ClusterEventListener listener, ClusterEventType... eventTypes) {
        ArgAssert.notNull(listener, "Listener");

        if (eventTypes == null || eventTypes.length == 0) {
            addListener(listener);
        } else {
            final EnumSet<ClusterEventType> eventTypesSet = EnumSet.copyOf(Arrays.asList(eventTypes));

            doAddListener(new FilteredListener(eventTypesSet, listener));
        }
    }

    public void addListenerAsync(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        doAddListenerAsync(new FilteredListener(listener));
    }

    public void addListenerAsync(ClusterEventListener listener, ClusterEventType... eventTypes) {
        ArgAssert.notNull(listener, "Listener");

        if (eventTypes == null || eventTypes.length == 0) {
            addListenerAsync(listener);
        } else {
            final EnumSet<ClusterEventType> eventTypesSet = EnumSet.copyOf(Arrays.asList(eventTypes));

            doAddListenerAsync(new FilteredListener(eventTypesSet, listener));
        }
    }

    public void removeListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        FilteredListener filteredListener = new FilteredListener(null, listener);

        ExecutorService localWorker = worker;

        if (localWorker == null || inAsync.get() != null) {
            if (DEBUG) {
                log.debug("Unregistering cluster event listener [listener={}]", filteredListener);
            }

            listeners.remove(filteredListener);
        } else {
            if (DEBUG) {
                log.debug("Scheduling cluster event listener unregistration for asynchronous processing [listener={}]", filteredListener);
            }

            Future<?> future = localWorker.submit(() -> {
                if (DEBUG) {
                    log.debug("Processing asynchronous cluster event listener unregistration [listener={}]", listener);
                }

                listeners.remove(filteredListener);
            });

            try {
                Utils.getUninterruptedly(future);
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    log.error("Failed to unregister cluster event listener [listener={}]", listener, e.getCause());
                }
            }
        }
    }

    public void start(ThreadFactory threads) {
        assert threads != null : "Thread factory is null.";

        worker = Executors.newSingleThreadExecutor(threads);

        if (DEBUG) {
            log.debug("Started cluster event manager.");
        }
    }

    public void stop() {
        ExecutorService localWorker = worker;

        if (localWorker != null) {
            if (DEBUG) {
                log.debug("Stopping cluster event manager...");
            }

            worker = null;

            // Submit final task to clear listeners and topology after all other tasks got executed.
            localWorker.submit(() -> {
                listeners.clear();

                lastTopology = null;
            });

            if (DEBUG) {
                log.debug("Awaiting for cluster event manager thread termination...");
            }

            Utils.shutdown(localWorker).awaitUninterruptedly();

            if (DEBUG) {
                log.debug("Done awaiting for cluster event manager thread termination...");
            }
        }
    }

    private void doAddListener(FilteredListener listener) {
        Optional<Future<?>> future = doAddListenerAsync(listener);

        future.ifPresent(it -> {
            try {
                Utils.getUninterruptedly(it);
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    log.error("Failed to register cluster event listener [listener={}]", listener, e.getCause());
                }
            }
        });
    }

    private Optional<Future<?>> doAddListenerAsync(FilteredListener listener) {
        ExecutorService localWorker = worker;

        if (localWorker == null || inAsync.get() != null) {
            if (DEBUG) {
                log.debug("Registering cluster event listener [listener={}]", listener);
            }

            listeners.add(listener);

            return Optional.empty();
        } else {
            if (DEBUG) {
                log.debug("Scheduling cluster event listener registration for asynchronous processing [listener={}]", listener);
            }

            Future<?> future = localWorker.submit(() -> {
                inAsync.set(true);

                try {
                    if (DEBUG) {
                        log.debug("Processing asynchronous cluster event listener registration [listener={}]", listener);
                    }

                    listeners.add(listener);

                    if (lastTopology != null) {
                        try {
                            listener.onEvent(new ClusterJoinEvent(lastTopology));
                        } catch (Throwable t) {
                            log.error("Failed to notify cluster event listener [listener={}]", listener, t);
                        }
                    }
                } finally {
                    inAsync.remove();
                }
            });

            return Optional.of(future);
        }
    }
}
