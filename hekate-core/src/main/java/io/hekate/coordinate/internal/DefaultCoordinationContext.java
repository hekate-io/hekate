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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.coordinate.CoordinationBroadcastCallback;
import io.hekate.coordinate.CoordinationContext;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationMember;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DefaultCoordinationContext implements CoordinationContext {
    private static final Logger log = LoggerFactory.getLogger(DefaultCoordinationContext.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final CoordinationMember coordinator;

    private final List<CoordinationMember> members;

    @ToStringIgnore
    private final String name;

    @ToStringIgnore
    private final ClusterTopology topology;

    @ToStringIgnore
    private final CoordinationHandler handler;

    @ToStringIgnore
    private final CoordinationMember localMember;

    @ToStringIgnore
    private final Map<ClusterNodeId, DefaultCoordinationMember> membersById;

    @ToStringIgnore
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    @ToStringIgnore
    private boolean prepared;

    private volatile Object attachment;

    public DefaultCoordinationContext(String name, ClusterTopology topology, MessagingChannel<CoordinationProtocol> channel,
        ExecutorService async, CoordinationHandler handler, long failoverDelay, Runnable onComplete) {
        assert name != null : "Process name is null.";
        assert topology != null : "Topology is null.";
        assert handler != null : "Coordination handler is null.";

        this.name = name;
        this.topology = topology;
        this.handler = handler;

        future.thenRun(onComplete);

        membersById = new HashMap<>(topology.size(), 1.0f);

        topology.getNodes().forEach(node -> {
            // First node is the coordinator.
            boolean coordinator = membersById.isEmpty();

            DefaultCoordinationMember member = new DefaultCoordinationMember(name, node, topology, coordinator, channel, async,
                failoverDelay);

            membersById.put(node.getId(), member);
        });

        this.members = Collections.unmodifiableList(new ArrayList<>(membersById.values()));

        Optional<CoordinationMember> localMemberOpt = members.stream().filter(m -> m.getNode().isLocal()).findFirst();
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
    public CoordinationMember getLocalMember() {
        return localMember;
    }

    @Override
    public boolean isCoordinator() {
        return localMember.isCoordinator();
    }

    @Override
    public CoordinationMember getCoordinator() {
        return coordinator;
    }

    @Override
    public ClusterTopology getTopology() {
        return topology;
    }

    @Override
    public List<CoordinationMember> getMembers() {
        return members;
    }

    @Override
    public CoordinationMember getMember(ClusterNode node) {
        ArgAssert.notNull(node, "Cluster node ");

        return getMember(node.getId());
    }

    @Override
    public CoordinationMember getMember(ClusterNodeId nodeId) {
        return membersById.get(nodeId);
    }

    @Override
    public int getSize() {
        return members.size();
    }

    @Override
    public void complete() {
        if (future.complete(null)) {
            if (DEBUG) {
                log.debug("Completed [context={}]", this);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getAttachment() {
        return (T)attachment;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    public void coordinate() {
        if (!future.isCancelled()) {
            if (DEBUG) {
                log.debug("Preparing [context={}]", this);
            }

            prepared = true;

            handler.prepare(this);

            if (isCoordinator()) {
                if (DEBUG) {
                    log.debug("Coordinating [context={}]", this);
                }

                handler.coordinate(this);
            } else {
                if (DEBUG) {
                    log.debug("Local node is not a coordinator [context={}]", this);
                }
            }
        }
    }

    public void processMessage(Message<CoordinationProtocol> msg) {
        CoordinationProtocol.Request request = msg.get(CoordinationProtocol.Request.class);

        boolean reject = false;

        if (!prepared) {
            reject = true;

            if (DEBUG) {
                log.debug("Rejected coordination request (context not prepared) [message={}, context={}]", request, this);
            }
        } else if (future.isCancelled()) {
            reject = true;

            if (DEBUG) {
                log.debug("Rejected coordination request (context cancelled) [message={}, context={}]", request, this);
            }
        } else if (!topology.getHash().equals(request.getTopology())) {
            reject = true;

            if (DEBUG) {
                log.debug("Rejected coordination request (topology mismatch) [message={}, context={}]", request, this);
            }
        }

        if (reject) {
            msg.reply(CoordinationProtocol.Reject.INSTANCE);
        } else {
            if (DEBUG) {
                log.debug("Processing coordination request [message={}, context={}]", request, this);
            }

            DefaultCoordinationMember member = membersById.get(request.getFrom());

            handler.process(new DefaultCoordinationRequest(name, member, msg), this);
        }
    }

    public void halt() {
        if (prepared && future.isCancelled()) {
            if (DEBUG) {
                log.debug("Halted [context={}]", this);
            }

            handler.cancel(this);
        }
    }

    public void cancel() {
        if (future.cancel(false)) {
            if (DEBUG) {
                log.debug("Cancelled [context={}]", this);
            }

            membersById.values().forEach(DefaultCoordinationMember::cancel);
        }
    }

    @Override
    public String toString() {
        return ToString.format(CoordinationContext.class, this);
    }
}
