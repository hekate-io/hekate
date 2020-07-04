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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.coordinate.CoordinationBroadcastCallback;
import io.hekate.coordinate.CoordinationContext;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationMember;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinatorContext;
import io.hekate.core.Hekate;
import io.hekate.core.HekateSupport;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

class DefaultCoordinatorContext implements CoordinatorContext {
    /**
     * Coordination context status.
     */
    private enum Status {
        /** New context. */
        NEW(false, false),

        /** See {@link #doPrepare()}. */
        PREPARED(false, false),

        /** See {@link #doComplete()}. */
        COMPLETED(true, false),

        /** See {@link #cancel()}. */
        CANCELLED(true, true);

        private final boolean done;

        private final boolean cancelled;

        Status(boolean done, boolean cancelled) {
            this.done = done;
            this.cancelled = cancelled;
        }

        public boolean isDone() {
            return done;
        }

        public boolean isCancelled() {
            return cancelled;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinatorContext.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    /** Coordinator node. */
    private final CoordinationMember coordinator;

    /** Epoch of this coordination. */
    private final CoordinationEpoch epoch;

    /** Topology of this coordination. */
    private final ClusterTopology topology;

    /** All members of this coordination. */
    @ToStringIgnore
    private final List<CoordinationMember> members;

    /** Container of this coordination. */
    @ToStringIgnore
    private final HekateSupport hekate;

    /** Coordination process name (see {@link CoordinationProcessConfig#setName(String)}). */
    @ToStringIgnore
    private final String name;

    /** Coordination handler (see {@link CoordinationProcessConfig#setHandler(CoordinationHandler)}). */
    @ToStringIgnore
    private final CoordinationHandler handler;

    /** Local node. */
    @ToStringIgnore
    private final DefaultCoordinationMember localMember;

    /** All members by their cluster IDs. */
    @ToStringIgnore
    private final Map<ClusterNodeId, DefaultCoordinationMember> membersById;

    /** Status of this coordination. */
    private final AtomicReference<Status> status = new AtomicReference<>(Status.NEW);

    /** Action to be executed upon successful completion of this coordination. */
    @ToStringIgnore
    private final Runnable onComplete;

    /** See {@link #setAttachment(Object)}. */
    @ToStringIgnore
    private volatile Object attachment;

    public DefaultCoordinatorContext(
        String name,
        HekateSupport hekate,
        CoordinationEpoch epoch,
        ClusterTopology topology,
        MessagingChannel<CoordinationProtocol> channel,
        ExecutorService async,
        CoordinationHandler handler,
        Runnable onComplete
    ) {
        this.name = name;
        this.hekate = hekate;
        this.epoch = epoch;
        this.topology = topology;
        this.handler = handler;
        this.onComplete = onComplete;

        membersById = new HashMap<>(topology.size(), 1.0f);

        topology.nodes().forEach(node -> {
            DefaultCoordinationMember member = new DefaultCoordinationMember(
                name,
                node,
                epoch,
                topology,
                channel,
                async
            );

            membersById.put(node.id(), member);
        });

        this.members = unmodifiableList(new ArrayList<>(membersById.values()));

        this.localMember = membersById.values().stream()
            .filter(m -> m.node().isLocal())
            .findFirst()
            .orElseThrow(() ->
                new IllegalArgumentException("No local node in the coordination topology [topology=" + topology + ", epoch=" + epoch + ']')
            );

        this.coordinator = members.stream()
            .filter(CoordinationMember::isCoordinator)
            .findFirst()
            .orElseThrow(() ->
                new IllegalArgumentException("No coordinator in the coordination topology [topology=" + topology + ", epoch=" + epoch + ']')
            );
    }

    public CoordinationEpoch epoch() {
        return epoch;
    }

    @Override
    public Hekate hekate() {
        return hekate.hekate();
    }

    @Override
    public void broadcast(Object request, CoordinationBroadcastCallback callback) {
        broadcast(request, all -> true, callback);
    }

    @Override
    public void broadcast(Object request, Predicate<CoordinationMember> filter, CoordinationBroadcastCallback callback) {
        broadcast(request, () -> true, filter, callback);
    }

    @Override
    public void broadcast(
        Object request,
        BooleanSupplier preCondition,
        Predicate<CoordinationMember> filter,
        CoordinationBroadcastCallback callback
    ) {
        if (preCondition.getAsBoolean()) {
            List<CoordinationMember> receivers = members.stream()
                .filter(filter)
                .collect(toList());

            if (!receivers.isEmpty()) {
                if (DEBUG) {
                    log.debug("Broadcasting [request={}, context={}]", request, this);
                }

                CoordinationBroadcastAdaptor adaptor = new CoordinationBroadcastAdaptor(receivers.size(), callback);

                receivers.forEach(member ->
                    member.request(request, adaptor)
                );

                return;
            }
        }

        // Nothing got submitted -> notify the callback.
        callback.onResponses(emptyMap());
    }

    @Override
    public boolean isDone() {
        return status.get().isDone();
    }

    @Override
    public boolean isCancelled() {
        return status.get().isCancelled();
    }

    @Override
    public CoordinationMember localMember() {
        return localMember;
    }

    @Override
    public boolean isCoordinator() {
        return localMember.isCoordinator();
    }

    @Override
    public CoordinationMember coordinator() {
        return coordinator;
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    @Override
    public List<CoordinationMember> members() {
        return members;
    }

    @Override
    public CoordinationMember memberOf(ClusterNode node) {
        ArgAssert.notNull(node, "Cluster node ");

        return memberOf(node.id());
    }

    @Override
    public CoordinationMember memberOf(ClusterNodeId nodeId) {
        return membersById.get(nodeId);
    }

    @Override
    public int size() {
        return members.size();
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public void coordinate() {
        if (!isDone()) {
            if (DEBUG) {
                log.debug("Preparing to coordinate [context={}]", this);
            }

            // Broadcast 'prepare' message to all members.
            broadcastPrepare(ignore -> {
                // Coordinate once we've got 'prepare' confirmations from all members.
                if (!isDone()) {
                    if (DEBUG) {
                        log.debug("Coordinating [context={}]", this);
                    }

                    handler.coordinate(this);
                }
            });
        }
    }

    public void processMessage(Message<CoordinationProtocol> msg) {
        CoordinationProtocol.RequestBase request = msg.payload(CoordinationProtocol.RequestBase.class);

        if (!epoch.equals(request.epoch())) {
            if (DEBUG) {
                log.debug("Rejected coordination request (topology mismatch) [message={}, context={}]", request, this);
            }

            msg.reply(CoordinationProtocol.Reject.INSTANCE);
        } else if (isDone()) {
            if (DEBUG) {
                log.debug("Rejected coordination request (context cancelled) [message={}, context={}]", request, this);
            }

            msg.reply(CoordinationProtocol.Reject.INSTANCE);
        } else {
            switch (request.type()) {
                case PREPARE: {
                    if (DEBUG) {
                        log.debug("Processing prepare request [message={}, context={}]", request, this);
                    }

                    doPrepare();

                    msg.reply(CoordinationProtocol.Confirm.INSTANCE);

                    break;
                }
                case REQUEST: {
                    if (DEBUG) {
                        log.debug("Processing coordination request [message={}, context={}]", request, this);
                    }

                    DefaultCoordinationMember member = membersById.get(request.from());

                    handler.process(new DefaultCoordinationRequest(name, member, msg), this);

                    break;
                }
                case COMPLETE:
                    if (DEBUG) {
                        log.debug("Processing complete request [message={}, context={}]", request, this);
                    }

                    doComplete();

                    msg.reply(CoordinationProtocol.Confirm.INSTANCE);

                    break;
                case RESPONSE:
                case CONFIRM:
                case REJECT:
                default: {
                    throw new IllegalArgumentException("Unexpected request type: " + request);
                }
            }
        }
    }

    @Override
    public void complete() {
        if (!isDone()) {
            // Broadcast 'complete' message to remote members.
            broadcastCompleteToRemotes(ignore -> {
                // Send 'complete' message to self.
                // --------------------------------------------------------------------------------------------------
                // We need to do it as the last step, because otherwise the local context may switch to the COMPLETED
                // state before it gets all confirmations from the remote members.
                // In such case, it will stop trying to resend messages (f.e. in case of a temporary network error)
                // and therefore some of the remote members will never become COMPLETED.
                localMember.sendComplete((response, from) -> {
                    // No-op.
                });
            });
        }
    }

    public void cancel() {
        if (status.compareAndSet(Status.PREPARED, Status.CANCELLED)) {
            if (DEBUG) {
                log.debug("Cancelling [context={}]", this);
            }

            try {
                handler.cancel(this);
            } finally {
                membersById.values().forEach(DefaultCoordinationMember::dispose);
            }
        }
    }

    private void doPrepare() {
        if (status.compareAndSet(Status.NEW, Status.PREPARED)) {
            if (DEBUG) {
                log.debug("Preparing [context={}]", this);
            }

            handler.prepare(this);
        }
    }

    private void doComplete() {
        if (status.compareAndSet(Status.PREPARED, Status.COMPLETED)) {
            if (DEBUG) {
                log.debug("Completed [context={}]", this);
            }

            try {
                handler.complete(this);
            } finally {
                membersById.values().forEach(DefaultCoordinationMember::dispose);

                onComplete.run();
            }
        }
    }

    private void broadcastPrepare(CoordinationBroadcastCallback callback) {
        CoordinationBroadcastAdaptor adaptor = new CoordinationBroadcastAdaptor(membersById.size(), callback);

        membersById.forEach((id, member) ->
            member.sendPrepare(adaptor)
        );
    }

    private void broadcastCompleteToRemotes(CoordinationBroadcastCallback callback) {
        List<DefaultCoordinationMember> remotes = membersById.values().stream()
            .filter(it -> !it.node().equals(localMember.node()))
            .collect(toList());

        if (remotes.isEmpty()) {
            callback.onResponses(emptyMap());
        } else {
            CoordinationBroadcastAdaptor adaptor = new CoordinationBroadcastAdaptor(remotes.size(), callback);

            remotes.forEach(member ->
                member.sendComplete(adaptor)
            );
        }
    }

    @Override
    public String toString() {
        return ToString.format(CoordinationContext.class, this);
    }
}
