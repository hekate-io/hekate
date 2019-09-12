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

package io.hekate.core.internal;

import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.cluster.event.ClusterLeaveReason;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.HekateSupport;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyList;

class ClusterEventManager implements HekateSupport {
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
            if (eventTypes == null || eventTypes.contains(event.type())) {
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

            return Objects.equals(delegate, that.delegate);
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

    private static final boolean TRACE = log.isTraceEnabled();

    private final StateGuard guard = new StateGuard(ClusterEventManager.class);

    private final Hekate hekate;

    private final List<FilteredListener> listeners = new CopyOnWriteArrayList<>();

    private final ThreadLocal<Boolean> insideWorker = new ThreadLocal<>();

    private ExecutorService worker;

    private ClusterTopology lastTopology;

    private volatile boolean joinEventFired;

    public ClusterEventManager(Hekate hekate) {
        this.hekate = hekate;
    }

    public CompletableFuture<Void> fireAsync(ClusterEvent event) {
        return guard.withWriteLock(() -> {
            if (DEBUG) {
                log.debug("Scheduled cluster event for asynchronous processing [event={}]", event);
            }

            if (event.type() == ClusterEventType.JOIN) {
                joinEventFired = true;
            } else if (event.type() == ClusterEventType.LEAVE) {
                joinEventFired = false;
            }

            CompletableFuture<Void> future = new CompletableFuture<>();

            worker.execute(() -> {
                insideWorker.set(true);

                try {
                    if (DEBUG) {
                        log.debug("Notifying listeners on cluster event [listeners={}, event={}]", listeners.size(), event);
                    }

                    lastTopology = event.topology();

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
                    insideWorker.remove();
                }
            });

            return future;
        });
    }

    public CompletableFuture<Void> ensureLeaveEventFired(ClusterLeaveReason reason, ClusterTopology topology) {
        return guard.withWriteLock(() -> {
            // If join event had been fired then we need to fire leave event too.
            if (joinEventFired) {
                ClusterLeaveEvent event = new ClusterLeaveEvent(reason, topology, emptyList(), emptyList(), this);

                return fireAsync(event);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        });
    }

    public boolean isJoinEventFired() {
        return joinEventFired;
    }

    public void addListener(ClusterEventListener listener) {
        addListener(listener, (ClusterEventType[])null);
    }

    public void addListener(ClusterEventListener listener, ClusterEventType... eventTypes) {
        ArgAssert.notNull(listener, "Listener");

        if (eventTypes == null || eventTypes.length == 0) {
            doAddListener(new FilteredListener(listener));
        } else {
            EnumSet<ClusterEventType> eventTypesSet = EnumSet.copyOf(Arrays.asList(eventTypes));

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

        Future<?> future = guard.withReadLock(() -> {
            FilteredListener filtered = new FilteredListener(null, listener);

            if (worker == null || insideWorker.get() != null) {
                if (TRACE) {
                    log.trace("Unregistering cluster event listener [listener={}]", filtered);
                }

                listeners.remove(filtered);

                return CompletableFuture.completedFuture(null);
            } else {
                if (TRACE) {
                    log.trace("Scheduling cluster event listener unregistration for asynchronous processing [listener={}]", filtered);
                }

                return worker.submit(() -> {
                    if (TRACE) {
                        log.trace("Processing asynchronous cluster event listener unregistration [listener={}]", listener);
                    }

                    listeners.remove(filtered);
                });
            }
        });

        try {
            AsyncUtils.getUninterruptedly(future);
        } catch (ExecutionException e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to unregister cluster event listener [listener={}]", listener, e.getCause());
            }
        }
    }

    public void start(ThreadFactory threads) {
        assert threads != null : "Thread factory is null.";

        guard.withWriteLock(() -> {
            worker = Executors.newSingleThreadExecutor(threads);

            if (DEBUG) {
                log.debug("Started cluster event manager.");
            }
        });
    }

    public void stop() {
        Waiting shutdown = guard.withWriteLock(() -> {
            if (worker == null) {
                return Waiting.NO_WAIT;
            } else {
                try {
                    if (DEBUG) {
                        log.debug("Stopping cluster event manager...");
                    }

                    // Submit final task to clear listeners and topology after all other tasks got executed.
                    worker.submit(() -> {
                        listeners.clear();

                        lastTopology = null;
                    });

                    return AsyncUtils.shutdown(worker);
                } finally {
                    // Cleanup worker thread.
                    worker = null;
                }
            }
        });

        if (DEBUG) {
            log.debug("Awaiting for cluster event manager thread termination...");
        }

        shutdown.awaitUninterruptedly();

        if (DEBUG) {
            log.debug("Done awaiting for cluster event manager thread termination...");
        }
    }

    @Override
    public Hekate hekate() {
        return hekate;
    }

    private void doAddListener(FilteredListener listener) {
        Optional<Future<?>> future = doAddListenerAsync(listener);

        future.ifPresent(it -> {
            try {
                AsyncUtils.getUninterruptedly(it);
            } catch (ExecutionException e) {
                if (log.isErrorEnabled()) {
                    log.error("Failed to register cluster event listener [listener={}]", listener, e.getCause());
                }
            }
        });
    }

    private Optional<Future<?>> doAddListenerAsync(FilteredListener listener) {
        return guard.withReadLock(() -> {
            if (worker == null || insideWorker.get() != null) {
                if (TRACE) {
                    log.trace("Registering cluster event listener [listener={}]", listener);
                }

                listeners.add(listener);

                return Optional.empty();
            } else {
                if (TRACE) {
                    log.trace("Scheduling cluster event listener registration for asynchronous processing [listener={}]", listener);
                }

                CompletableFuture<?> future = new CompletableFuture<>();

                worker.submit(() -> {
                    insideWorker.set(true);

                    try {
                        if (TRACE) {
                            log.trace("Processing asynchronous cluster event listener registration [listener={}]", listener);
                        }

                        // Register the cluster event listener.
                        listeners.add(listener);

                        // Notify the future before trying to process the latest topology change event.
                        // Need to do it here in order to prevent a deadlock between the registering thread and the event processing thread.
                        future.complete(null);

                        // Check if the local node is already in the cluster.
                        if (lastTopology != null) {
                            // First event should always be the 'join' event.
                            ClusterJoinEvent event = new ClusterJoinEvent(lastTopology, this);

                            try {
                                if (DEBUG) {
                                    log.debug("Notifying listener on cluster event [listener={}, event={}]", listener, event);
                                }

                                listener.onEvent(event);
                            } catch (Throwable t) {
                                log.error("Failed to notify cluster event listener [listener={}]", listener, t);
                            }
                        }
                    } finally {
                        insideWorker.remove();
                    }
                });

                return Optional.of(future);
            }
        });
    }
}
