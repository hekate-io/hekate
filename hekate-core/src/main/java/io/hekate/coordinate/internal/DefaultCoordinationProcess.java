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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterTopology;
import io.hekate.coordinate.CoordinationFuture;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationProcess;
import io.hekate.coordinate.internal.CoordinationProtocol.Prepare;
import io.hekate.coordinate.internal.CoordinationProtocol.Reject;
import io.hekate.core.HekateSupport;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultCoordinationProcess implements CoordinationProcess {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinationProcess.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String name;

    private final boolean asyncInit;

    @ToStringIgnore
    private final CoordinationHandler handler;

    @ToStringIgnore
    private final ExecutorService async;

    @ToStringIgnore
    private final MessagingChannel<CoordinationProtocol> channel;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(DefaultCoordinationProcess.class);

    @ToStringIgnore
    private final CoordinationFuture future = new CoordinationFuture();

    @ToStringIgnore
    private final HekateSupport hekate;

    @ToStringIgnore
    private DefaultCoordinatorContext ctx;

    @ToStringIgnore
    private ClusterTopology topology;

    @ToStringIgnore
    private long epochIdSeq = 1;

    public DefaultCoordinationProcess(
        String name,
        HekateSupport hekate,
        CoordinationHandler handler,
        boolean asyncInit,
        ExecutorService async,
        MessagingChannel<CoordinationProtocol> channel
    ) {
        this.name = name;
        this.hekate = hekate;
        this.handler = handler;
        this.async = async;
        this.asyncInit = asyncInit;
        this.channel = channel;
    }

    public CompletableFuture<?> initialize() {
        return guard.withWriteLock(() -> {
            guard.becomeInitialized();

            CompletableFuture<?> initFuture = new CompletableFuture<>();

            async.execute(() -> {
                try {
                    if (DEBUG) {
                        log.debug("Initializing handler [process={}]", name);
                    }

                    handler.initialize();

                    initFuture.complete(null);
                } catch (Throwable e) {
                    initFuture.completeExceptionally(e);
                }
            });

            return initFuture;
        });
    }

    public Waiting terminate() {
        return guard.withWriteLock(() -> {
            if (guard.becomeTerminated()) {
                cancelCurrentContext();

                async.execute(() -> {
                    try {
                        if (DEBUG) {
                            log.debug("Terminating handler [process={}]", name);
                        }

                        handler.terminate();
                    } catch (Throwable e) {
                        log.error("Got an unexpected error during coordination handler termination [process={}]", name, e);
                    }
                });

                future.cancel(false);

                epochIdSeq = 1;
                ctx = null;
                topology = null;

                return AsyncUtils.shutdown(async);
            } else {
                return Waiting.NO_WAIT;
            }
        });
    }

    public void processTopologyChange(ClusterTopology newTopology) {
        guard.withWriteLockIfInitialized(() -> {
            if (DEBUG) {
                log.debug("Processing topology change [topology={}]", newTopology);
            }

            if (topology != null && topology.equals(newTopology)) {
                if (DEBUG) {
                    log.debug("Topology not changed [process={}]", name);
                }
            } else {
                this.topology = newTopology;

                cancelCurrentContext();

                if (shouldCoordinate(topology)) {
                    // Advance epoch ID.
                    long epochId = epochIdSeq++;

                    CoordinationEpoch epoch = new CoordinationEpoch(topology.localNode().id(), epochId);

                    // Prepare new coordinator context.
                    DefaultCoordinatorContext newCtx = newContext(epoch);

                    this.ctx = newCtx;

                    if (DEBUG) {
                        log.debug("Created new coordinator context [context={}]", newCtx);
                    }

                    async.execute(() -> {
                        try {
                            // Note we are using the captured context.
                            newCtx.coordinate();
                        } catch (Throwable e) {
                            log.error("Got an unexpected error during coordination [process={}]", name, e);
                        }
                    });
                }
            }
        });
    }

    public void processMessage(Message<CoordinationProtocol> msg) {
        if (msg.is(Prepare.class)) {
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Prepare.
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            Prepare prepare = msg.payload(Prepare.class);

            guard.withWriteLock(() -> {
                // Prepare only if we are initialized and our local topology is the same with the requested one.
                if (guard.isInitialized()
                    && topology != null
                    && topology.hash().equals(prepare.topologyHash())
                ) {
                    DefaultCoordinatorContext localCtx;

                    if (ctx != null && ctx.epoch().equals(prepare.epoch())) {
                        // Do not change the context as it is already initialized for the requested epoch (could be a message retransmit).
                        localCtx = this.ctx;
                    } else {
                        // Cancel old context.
                        cancelCurrentContext();

                        // Prepare new context.
                        localCtx = newContext(prepare.epoch());

                        this.ctx = localCtx;

                        if (DEBUG) {
                            log.debug("Created new coordinated context [context={}]", localCtx);
                        }
                    }

                    if (localCtx == null) {
                        reject(msg);
                    } else {
                        async.execute(() -> {
                            try {
                                // Note we are using the captured context.
                                localCtx.processMessage(msg);
                            } catch (Throwable e) {
                                log.error("Failed to process coordination request [message={}]", msg, e);

                                reject(msg);
                            }
                        });
                    }
                } else {
                    reject(msg);
                }
            });
        } else {
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // Other Messages.
            ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            guard.withReadLock(() -> {
                if (guard.isInitialized() && ctx != null) {
                    DefaultCoordinatorContext localCtx = this.ctx;

                    async.execute(() -> {
                        try {
                            // Note we are using the captured context.
                            localCtx.processMessage(msg);
                        } catch (Throwable e) {
                            log.error("Failed to process coordination request [message={}]", msg, e);

                            reject(msg);
                        }
                    });
                } else {
                    reject(msg);
                }
            });
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CoordinationFuture future() {
        return future.fork();
    }

    @Override
    public CoordinationHandler handler() {
        return handler;
    }

    public boolean isAsyncInit() {
        return asyncInit;
    }

    private DefaultCoordinatorContext newContext(CoordinationEpoch epoch) {
        return new DefaultCoordinatorContext(
            name,
            hekate,
            epoch,
            topology,
            channel,
            async,
            handler,
            () -> future.complete(this) // <-- When coordination is done.
        );
    }

    private void cancelCurrentContext() {
        assert guard.isWriteLocked() : "Must hold a write lock.";

        DefaultCoordinatorContext localCtx = this.ctx;

        if (localCtx != null) {
            async.execute(() -> {
                try {
                    localCtx.cancel();
                } catch (Throwable e) {
                    log.error("Got an unexpected error while canceling coordination [process={}]", name, e);
                }
            });
        }
    }

    private void reject(Message<CoordinationProtocol> msg) {
        if (msg.mustReply()) {
            if (DEBUG) {
                log.debug("Rejected coordination prepare request [message={}]", msg.payload());
            }

            msg.reply(Reject.INSTANCE);
        }
    }

    private static boolean shouldCoordinate(ClusterTopology topology) {
        // Coordinator is the first node in the cluster topology (note that nodes are pre-ordered).
        return !topology.isEmpty()
            && topology.nodes().get(0).isLocal();
    }

    @Override
    public String toString() {
        return ToString.format(CoordinationProcess.class, this);
    }
}
