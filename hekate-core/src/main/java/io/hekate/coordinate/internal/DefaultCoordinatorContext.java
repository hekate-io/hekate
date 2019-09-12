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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableList;

class DefaultCoordinatorContext implements CoordinatorContext {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinatorContext.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    /** Coordinator node. */
    private final CoordinationMember coordinator;

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
    private final CoordinationMember localMember;

    /** All members by their cluster IDs. */
    @ToStringIgnore
    private final Map<ClusterNodeId, DefaultCoordinationMember> membersById;

    /** Future of this coordination. */
    @ToStringIgnore
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    /** Is this context prepared (see {@link #ensurePrepared()}). */
    @ToStringIgnore
    private boolean prepared;

    /** See {@link #setAttachment(Object)}. */
    @ToStringIgnore
    private volatile Object attachment;

    public DefaultCoordinatorContext(
        String name,
        HekateSupport hekate,
        ClusterTopology topology,
        MessagingChannel<CoordinationProtocol> channel,
        ExecutorService async,
        CoordinationHandler handler,
        Runnable onComplete
    ) {
        assert hekate != null : "Hekate is null.";
        assert name != null : "Process name is null.";
        assert topology != null : "Topology is null.";
        assert handler != null : "Coordination handler is null.";

        this.name = name;
        this.hekate = hekate;
        this.topology = topology;
        this.handler = handler;

        future.thenRun(onComplete);

        membersById = new HashMap<>(topology.size(), 1.0f);

        topology.nodes().forEach(node -> {
            // First node is the coordinator.
            boolean coordinator = membersById.isEmpty();

            DefaultCoordinationMember member = new DefaultCoordinationMember(
                name,
                node,
                topology,
                coordinator,
                channel,
                async
            );

            membersById.put(node.id(), member);
        });

        this.members = unmodifiableList(new ArrayList<>(membersById.values()));

        Optional<CoordinationMember> localMemberOpt = members.stream().filter(m -> m.node().isLocal()).findFirst();
        Optional<CoordinationMember> coordinatorOpt = members.stream().filter(CoordinationMember::isCoordinator).findFirst();

        assert localMemberOpt.isPresent() : "Failed to find local node in the coordination topology [topology=" + topology + ']';
        assert coordinatorOpt.isPresent() : "Failed to find coordinator node in the coordination topology [topology=" + topology + ']';

        localMember = localMemberOpt.get();
        coordinator = coordinatorOpt.get();
    }

    @Override
    public void broadcast(Object request, CoordinationBroadcastCallback callback) {
        if (DEBUG) {
            log.debug("Broadcasting [request={}, context={}]", request, this);
        }

        BroadcastCallbackAdaptor callbackAdaptor = new BroadcastCallbackAdaptor(members.size(), callback);

        members.forEach(member ->
            member.request(request, callbackAdaptor)
        );
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
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
    public void complete() {
        if (!future.isDone()) {
            // Broadcast 'complete' message to remote members.
            broadcastCompleteToRemotes(ignore -> {
                if (!future.isDone()) {
                    // Complete this context once we've got confirmations from all remote members.
                    doComplete();
                }
            });
        }
    }

    @Override
    public Object getAttachment() {
        return attachment;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public void tryCoordinate() {
        if (!future.isDone()) {
            if (isCoordinator()) {
                if (DEBUG) {
                    log.debug("Preparing to coordinate [context={}]", this);
                }

                // Broadcast 'prepare' message to all members.
                broadcastPrepare(ignore -> {
                    // Coordinate once we've got 'prepare' confirmations from all members.
                    if (!future.isDone()) {
                        if (DEBUG) {
                            log.debug("Coordinating [context={}]", this);
                        }

                        handler.coordinate(this);
                    }
                });
            } else {
                if (DEBUG) {
                    log.debug("Local node is not a coordinator [context={}]", this);
                }
            }
        }
    }

    public void processMessage(Message<CoordinationProtocol> msg) {
        CoordinationProtocol.RequestBase request = msg.payload(CoordinationProtocol.RequestBase.class);

        boolean reject = false;

        if (!topology.hash().equals(request.topology())) {
            reject = true;

            if (DEBUG) {
                log.debug("Rejected coordination request (topology mismatch) [message={}, context={}]", request, this);
            }
        } else if (future.isDone()) {
            // Never reject 'prepare' and 'complete' requests even if local coordination is completed.
            // Such requests could be resubmitted because of network failures and we should always try to send back a confirmation.
            reject = request.type() != CoordinationProtocol.Type.PREPARE && request.type() != CoordinationProtocol.Type.COMPLETE;

            if (DEBUG) {
                log.debug("Rejected coordination request (context cancelled) [message={}, context={}]", request, this);
            }
        }

        if (reject) {
            msg.reply(CoordinationProtocol.Reject.INSTANCE);
        } else {
            switch (request.type()) {
                case PREPARE: {
                    if (DEBUG) {
                        log.debug("Processing prepare request [message={}, context={}]", request, this);
                    }

                    ensurePrepared();

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

    public void cancel() {
        if (future.cancel(false)) {
            if (DEBUG) {
                log.debug("Cancelled [context={}]", this);
            }

            membersById.values().forEach(DefaultCoordinationMember::dispose);
        }
    }

    public void postCancel() {
        if (prepared && future.isCancelled()) {
            if (DEBUG) {
                log.debug("Post-cancelled [context={}]", this);
            }

            handler.cancel(this);
        }
    }

    @Override
    public Hekate hekate() {
        return hekate.hekate();
    }

    private void doComplete() {
        if (future.complete(null)) {
            if (DEBUG) {
                log.debug("Completed [context={}]", this);
            }

            try {
                handler.complete(this);
            } finally {
                membersById.values().forEach(DefaultCoordinationMember::dispose);
            }
        }
    }

    private void broadcastPrepare(CoordinationBroadcastCallback callback) {
        BroadcastCallbackAdaptor callbackAdaptor = new BroadcastCallbackAdaptor(membersById.size(), callback);

        membersById.forEach((id, member) ->
            member.sendPrepare(callbackAdaptor)
        );
    }

    private void broadcastCompleteToRemotes(CoordinationBroadcastCallback callback) {
        List<DefaultCoordinationMember> remotes = membersById.values().stream()
            .filter(it -> !it.node().equals(localMember.node()))
            .collect(Collectors.toList());

        if (remotes.isEmpty()) {
            callback.onResponses(emptyMap());
        } else {
            BroadcastCallbackAdaptor callbackAdaptor = new BroadcastCallbackAdaptor(remotes.size(), callback);

            remotes.forEach(member ->
                member.sendComplete(callbackAdaptor)
            );
        }
    }

    private void ensurePrepared() {
        if (!prepared) {
            if (DEBUG) {
                log.debug("Preparing [context={}]", this);
            }

            prepared = true;

            handler.prepare(this);
        }
    }

    @Override
    public String toString() {
        return ToString.format(CoordinationContext.class, this);
    }
}
