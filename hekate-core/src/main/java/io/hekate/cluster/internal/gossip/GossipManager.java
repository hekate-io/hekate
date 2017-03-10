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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinAccept;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReject;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.Update;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateBase;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateDigest;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.cluster.internal.gossip.GossipNodeStatus.DOWN;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.JOINING;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.LEAVING;
import static io.hekate.cluster.internal.gossip.GossipNodeStatus.UP;
import static java.util.stream.Collectors.toList;

public class GossipManager {
    public static final int GOSSIP_FANOUT_SIZE = 3;

    private static final Logger log = LoggerFactory.getLogger(GossipManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final boolean TRACE = log.isTraceEnabled();

    private final String cluster;

    private final ClusterNode node;

    private final ClusterAddress address;

    private final ClusterNodeId id;

    private final FailureDetector healthManager;

    private final GossipNodesDeathWatch deathWatch;

    private final GossipListener listener;

    private final int speedUpGossipSize;

    private GossipNodeStatus status;

    private Gossip localGossip;

    private GossipSeedNodesSate seedNodesSate;

    private Set<ClusterNode> lastTopology = Collections.emptySet();

    private Set<ClusterAddress> knownAddresses = Collections.emptySet();

    private boolean leaveScheduled;

    public GossipManager(String cluster, ClusterNode node, FailureDetector healthManager, int speedUpGossipSize, GossipListener listener) {
        assert cluster != null : "Cluster is null.";
        assert node != null : "Local node is null.";
        assert healthManager != null : "Health manager is null.";
        assert listener != null : "Listener is null.";

        this.cluster = cluster;
        this.node = node;
        this.address = node.getAddress();
        this.healthManager = healthManager;
        this.listener = listener;
        this.speedUpGossipSize = speedUpGossipSize;

        id = node.getId();

        status = DOWN;

        updateLocalGossip(new Gossip());

        int failureQuorum = Math.max(1, healthManager.getFailureDetectionQuorum());

        deathWatch = new GossipNodesDeathWatch(id, failureQuorum, 0);
    }

    public Gossip getLocalGossip() {
        return localGossip;
    }

    public JoinRequest join(List<InetSocketAddress> seedNodes) {
        assert seedNodes != null : "Seed nodes list is null.";

        if (localGossip.hasMembers()) {
            // Already joined.
            return null;
        }

        if (DEBUG) {
            log.debug("Joining cluster [seed-nodes={}]", seedNodes);
        }

        if (seedNodesSate == null) {
            seedNodesSate = new GossipSeedNodesSate(address.getNetAddress(), seedNodes);
        } else {
            seedNodesSate.update(seedNodes);
        }

        return tryJoin(true);
    }

    public JoinRequest processJoinReject(JoinReject msg) {
        assert msg != null : "Message is null.";
        assert msg.getTo().equals(address) : "Message is addressed to another node [message=" + msg + ']';

        if (localGossip.hasMembers()) {
            if (DEBUG) {
                log.debug("Skipped join reject since local node is already joined [reply={}, gossip={}]", msg, localGossip);
            }

            return null;
        }

        if (DEBUG) {
            log.debug("Processing a join reject message [message={}]", msg);
        }

        switch (msg.getRejectType()) {
            case TEMPORARY: {
                seedNodesSate.onReject(msg.getRejectedAddress());

                return tryJoin(false);
            }
            case PERMANENT: {
                seedNodesSate.onBan(msg.getRejectedAddress());

                return tryJoin(false);
            }
            case FATAL: {
                listener.onJoinReject(msg.getFrom(), msg.getReason());

                return null;
            }
            default: {
                throw new IllegalArgumentException("Unexpected reject type: " + msg.getRejectType());
            }
        }
    }

    public JoinRequest processJoinFailure(JoinRequest msg) {
        assert msg != null : "Message is null.";

        if (localGossip.hasMembers()) {
            if (DEBUG) {
                log.debug("Skipped join message failure since local node is already joined [reply={}, gossip={}]", msg, localGossip);
            }

            return null;
        }

        if (DEBUG) {
            log.debug("Processing join request failure [message={}]", msg);
        }

        seedNodesSate.onFailure(msg.getToAddress());

        return tryJoin(false);
    }

    public Update processJoinAccept(JoinAccept msg) {
        assert msg != null : "Message is null.";
        assert msg.getTo().equals(address) : "Message is addressed to another node [message=" + msg + ']';

        Gossip remote = msg.getGossip();

        if (!remote.hasMember(id)) {
            if (DEBUG) {
                log.debug("Skipped join reply since local node is not in members list [reply={}, gossip={}]", msg, localGossip);
            }
        } else if (localGossip.hasMembers()) {
            if (DEBUG) {
                log.debug("Skipped join reply since local node is already joined [reply={}, gossip={}]", msg, localGossip);
            }
        } else {
            if (DEBUG) {
                log.debug("Processing join reply [reply={}]", msg);
            }

            updateLocalGossip(remote.seen(id));

            updateLocalSate();

            Update response = new Update(node.getAddress(), msg.getFrom(), localGossip);

            if (DEBUG) {
                log.debug("Created gossip update [update={}]", response);
            }

            return response;
        }

        return null;
    }

    public JoinReject acceptJoinRequest(JoinRequest msg) {
        assert msg != null : "Message is null.";

        if (!cluster.equals(msg.getCluster())) {
            if (DEBUG) {
                log.debug("Permanently rejected join request since this node belongs to another cluster [request={}]", msg);
            }

            return JoinReject.permanent(address, msg.getFrom(), msg.getToAddress());
        } else if (msg.getFromNode().getId().equals(id)) {
            if (DEBUG) {
                log.debug("Permanently rejected join request from self [request={}]", msg);
            }

            return JoinReject.permanent(address, msg.getFrom(), msg.getToAddress());
        } else if (!localGossip.hasMembers()) {
            if (DEBUG) {
                log.debug("Rejected join request since local node is not joined yet [request={}]", msg);
            }

            return JoinReject.retryLater(address, msg.getFrom(), msg.getToAddress());
        } else if (status == LEAVING || status == DOWN) {
            if (DEBUG) {
                log.debug("Rejected join request since local node is in {} state [request={}]", status, msg);
            }

            // Do not reject permanently since another node can be started on the same address once this node is stopped.
            return JoinReject.retryLater(address, msg.getFrom(), msg.getToAddress());
        }

        return null;
    }

    public JoinReply processJoinRequest(JoinRequest msg) {
        if (DEBUG) {
            log.debug("Processing join request [request={}]", msg);
        }

        JoinReject reject = acceptJoinRequest(msg);

        if (reject != null) {
            return reject;
        }

        ClusterNode joining = msg.getFromNode();

        if (!localGossip.hasMember(joining.getId())) {
            GossipNodeState newMember = new GossipNodeState(joining, JOINING);

            updateLocalGossip(localGossip.update(id, newMember));

            updateWatchNodes();
        }

        JoinAccept accept = new JoinAccept(address, joining.getAddress(), localGossip);

        if (DEBUG) {
            log.debug("Created join accept reply [reply={}]", accept);
        }

        return accept;
    }

    public JoinReject reject(JoinRequest msg, String reason) {
        return JoinReject.fatal(address, msg.getFrom(), msg.getToAddress(), reason);
    }

    public Collection<UpdateBase> batchGossip(GossipPolicy policy) {
        int batchSize;

        if (localGossip.getSeen().size() < localGossip.getMembers().size()) {
            batchSize = GOSSIP_FANOUT_SIZE;
        } else {
            batchSize = 1;
        }

        Collection<UpdateBase> msgs = tryCoordinateAndGossip(batchSize, policy);

        if (DEBUG) {
            if (msgs.isEmpty()) {
                log.trace("No nodes to gossip.");
            } else {
                msgs.forEach(msg -> log.debug("Will gossip [gossip={}]", msg));
            }
        }

        return msgs;
    }

    public UpdateBase gossip() {
        UpdateBase msg = tryCoordinateAndGossip(1, GossipPolicy.RANDOM_PREFER_UNSEEN).stream()
            .findFirst()
            .orElse(null);

        if (DEBUG) {
            if (msg == null) {
                log.debug("No nodes to gossip.");
            } else {
                log.debug("Will gossip [gossip={}]", msg);
            }
        }

        return msg;
    }

    public UpdateBase processUpdate(UpdateBase msg) {
        assert msg != null : "Message is null.";
        assert msg.getTo().equals(address) : "Message is addressed to another node [message=" + msg + ']';

        long thisVer = localGossip.getVersion();
        long otherVer = msg.getGossipBase().getVersion();

        if (status == DOWN) {
            if (DEBUG) {
                log.debug("Skipped gossip message since local node is in {} state [message={}]", DOWN, msg);
            }

            return null;
        }

        // Check gossip state version.
        // If versions diverge by more than 1 then we can't track causal history of the gossip state
        // since some DOWN nodes could be completely purged from the gossip state.
        if (Math.abs(thisVer - otherVer) > 1) {
            if (thisVer < otherVer) {
                // Local node is in inconsistent state.
                if (DEBUG) {
                    log.debug("Notifying listener on inconsistency caused by the gossip version mismatch "
                        + "[local={}, remote={}]", localGossip, msg);
                }

                listener.onNodeInconsistency(status);

                return null;
            } else {
                // Remote node is in inconsistent state.
                if (DEBUG) {
                    log.debug("Sending the gossip update digest because of a gossip version mismatch "
                        + "[local={}, remote={}]", localGossip, msg);
                }

                // Notify remote node by simply sending a gossip state digest.
                // Remote node will judge itself as being inconsistent by performing the same checks as we did above.
                return new UpdateDigest(address, msg.getFrom(), new GossipDigest(localGossip));
            }
        }

        GossipBase remote = msg.getGossipBase();

        if (!remote.hasMember(id)) {
            // Check if local node wasn't remove from the cluster.
            // Can happen in case of a long GC pause on this node.
            // Such pause can make other members to think that this node is dead and remove it from cluster.
            if (remote.getRemoved().contains(id)) {
                if (DEBUG) {
                    log.debug("Notifying listener on inconsistency since local node is in removed set "
                        + "[local={}, remote={}]", localGossip, msg);
                }

                listener.onNodeInconsistency(status);
            } else {
                if (DEBUG) {
                    log.debug("Skipped gossip message since local node is not on the members list [message={}]", msg);
                }
            }

            return null;
        }

        if (DEBUG) {
            log.debug("Processing gossip message [message={}]", msg);
        }

        ComparisonResult comparison = localGossip.compare(remote);

        if (DEBUG) {
            log.debug("Compared gossip versions [comparison-result={}, local={}, remote={}]", comparison, localGossip, remote);
        }

        boolean needToReply;

        boolean replyWithDigest = false;
        boolean seenChanged = false;
        boolean gossipStateChanged = false;

        switch (comparison) {
            case BEFORE: {
                // Local is older.
                needToReply = !remote.hasSeen(id);

                Update update = msg.asUpdate();

                if (update != null) {
                    Gossip remoteGossip = update.getGossip();

                    updateLocalGossip(remoteGossip.inheritSeen(id, localGossip));

                    gossipStateChanged = true;

                    if (DEBUG) {
                        log.debug("Updated local gossip [gossip={}]", localGossip);
                    }
                } else {
                    replyWithDigest = true;
                }

                break;
            }
            case AFTER: {
                // Local is newer.
                needToReply = true;

                Gossip updatedSeen = localGossip.inheritSeen(id, remote);

                if (DEBUG) {
                    // We are checking JVM identity to make sure that gossip was really changed.
                    if (updatedSeen != localGossip) {
                        log.debug("Updated local seen list [gossip={}]", localGossip);
                    }
                }

                updateLocalGossip(updatedSeen);

                break;
            }
            case SAME: {
                // Both are equals.
                needToReply = !remote.hasSeen(id);

                if (needToReply) {
                    replyWithDigest = true;
                }

                Set<ClusterNodeId> remoteSeen = remote.getSeen();

                if (!localGossip.hasSeen(remoteSeen)) {
                    updateLocalGossip(localGossip.seen(remoteSeen));

                    seenChanged = true;

                    if (DEBUG) {
                        log.debug("Updated local seen list [gossip={}]", localGossip);
                    }
                }

                break;
            }
            case CONCURRENT: {
                // Need to merge.
                needToReply = true;

                Update update = msg.asUpdate();

                // Merge if update is available.
                // Otherwise full gossip will be send back and merge will happen on the sender side.
                if (update != null) {
                    Gossip remoteGossip = update.getGossip();

                    Gossip merged = localGossip.merge(id, remoteGossip);

                    if (DEBUG) {
                        log.debug("Merged gossips [merged={}, local={}, remote={}]", merged, localGossip, remote);
                    }

                    updateLocalGossip(merged);

                    gossipStateChanged = true;
                }

                break;
            }
            default: {
                throw new IllegalStateException("Unexpected comparison result: " + comparison);
            }
        }

        // Check if local node wasn't remove from the cluster.
        // Can happen in case of a long GC pause on this node.
        // Such pause can make other members to think that this node is dead and remove it from cluster.
        if (!leaveScheduled && localGossip.getMember(id).getStatus() == DOWN) {
            if (DEBUG) {
                log.debug("Notifying listener on inconsistency since local node is seen as {} by remote node "
                    + "[remote-node={}]", DOWN, msg.getFrom());
            }

            listener.onNodeInconsistency(status);

            return null;
        }

        // May be coordinate.
        if (tryCoordinate()) {
            needToReply = true;
            gossipStateChanged = true;
        }

        // Update local gossip state if anything have changed.
        if (gossipStateChanged || seenChanged) {
            if (updateLocalSate()) {
                needToReply = true;
            }
        }

        // Send back the reply (if needed).
        if (needToReply) {
            UpdateBase reply;

            if (replyWithDigest) {
                reply = new UpdateDigest(address, msg.getFrom(), new GossipDigest(localGossip));
            } else {
                reply = new Update(address, msg.getFrom(), localGossip);
            }

            if (DEBUG) {
                log.debug("Created gossip reply [reply={}]", reply);
            }

            return reply;
        }

        // Check if speed up should be applied.
        if (status != DOWN && localGossip.getMembers().size() <= speedUpGossipSize) {
            UpdateBase speedUp = null;

            // Select speed up coordinator node.
            ClusterNode coordinator = localGossip.getCoordinator(id);

            if (coordinator != null) {
                if (coordinator.equals(node)) {
                    // Local node is the coordinator.
                    // Try gossip to any NON DOWN node that haven't seen this state.
                    speedUp = doGossip(1, GossipPolicy.RANDOM_UNSEEN_NON_DOWN).stream().findFirst().orElse(null);

                    if (speedUp == null) {
                        // Try gossip to any DOWN node that haven't seen this state.
                        speedUp = doGossip(1, GossipPolicy.RANDOM_UNSEEN).stream().findFirst().orElse(null);
                    }
                } else {
                    // Remote node is the coordinator.
                    if (!localGossip.hasSeen(coordinator.getId())) {
                        // Send update directly to coordinator if he hasn't seen this gossip version.
                        speedUp = new Update(address, coordinator.getAddress(), localGossip);
                    }

                    if (speedUp == null) {
                        // Send gossip digest directly to coordinator if seen list was updated during the message processing.
                        if (seenChanged) {
                            speedUp = new UpdateDigest(address, coordinator.getAddress(), new GossipDigest(localGossip));
                        }
                    }
                }
            }

            if (speedUp != null) {
                if (DEBUG) {
                    log.debug("Speed up gossip [gossip={}]", speedUp);
                }

                return speedUp;
            }
        }

        return null;
    }

    public boolean checkAliveness() {
        boolean suspectsChanged = false;

        if (localGossip.hasMembers()) {
            if (TRACE) {
                log.trace("Checking nodes aliveness using {}", healthManager);
            }

            GossipNodeState localNodeState = localGossip.getMember(id);

            Set<ClusterNodeId> oldSuspects = localNodeState.getSuspected();

            Set<ClusterNodeId> newSuspects = new HashSet<>();

            localGossip.stream().forEach(m -> {
                ClusterNodeId nodeId = m.getId();

                if (!id.equals(nodeId)) {
                    boolean isAlive = healthManager.isAlive(m.getNodeAddress());

                    if (!isAlive) {
                        if (!oldSuspects.contains(nodeId)
                            // It is possible that failed node became DOWN and stopped before this node received this information.
                            && m.getStatus() != DOWN && m.getStatus() != LEAVING) {
                            listener.onNodeFailureSuspected(m.getNode(), m.getStatus());
                        }

                        newSuspects.add(nodeId);
                    } else if (oldSuspects.contains(nodeId) && m.getStatus() != DOWN) {
                        listener.onNodeFailureUnsuspected(m.getNode(), m.getStatus());
                    }
                }
            });

            if (!newSuspects.equals(oldSuspects)) {
                if (DEBUG) {
                    log.debug("Updating local suspects table [new={}, old={}]", newSuspects, oldSuspects);
                }

                suspectsChanged = true;

                updateLocalGossip(localGossip.update(id, localNodeState.suspected(newSuspects)));
            }

            deathWatch.update(localGossip);

            if (localGossip.isCoordinator(id)) {
                if (TRACE) {
                    log.trace("Checking for terminated nodes.");
                }

                List<ClusterNodeId> terminated = deathWatch.terminateNodes();

                // Set state of terminated nodes to DOWN.
                if (!terminated.isEmpty()) {
                    List<GossipNodeState> updated = terminated.stream()
                        .filter(terminatedId -> {
                            GossipNodeState member = localGossip.getMember(terminatedId);

                            return member != null && member.getStatus() != DOWN;
                        })
                        .map(terminatedId -> {
                            GossipNodeState from = localGossip.getMember(terminatedId);

                            GossipNodeState to = from.status(DOWN);

                            if (DEBUG) {
                                log.debug("Terminating node [node={}, state={}]", from.getNode(), from.getStatus());
                            }

                            listener.onNodeFailure(from.getNode(), from.getStatus());

                            return to;
                        })
                        .collect(toList());

                    updateLocalGossip(localGossip.update(id, updated));

                    if (DEBUG) {
                        log.debug("Updated local gossip [gossip={}]", localGossip);
                    }
                }
            }
        }

        return suspectsChanged;
    }

    public UpdateBase leave() {
        if (status == LEAVING) {
            if (DEBUG) {
                log.debug("Skipped leaving since local node is in {} state [gossip={}]", status, localGossip);
            }
        } else if (status == DOWN || status == JOINING && !localGossip.isConvergent()) {
            leaveScheduled = true;

            if (DEBUG) {
                log.debug("Scheduled leave operation to be executed after join since local node is in {} state.", status);
            }
        } else {
            leaveScheduled = true;

            GossipNodeState newState = localGossip.getMember(id).status(LEAVING);

            updateLocalGossip(localGossip.update(id, newState));

            if (DEBUG) {
                log.debug("Leaving cluster [gossip={}]", localGossip);
            }

            updateLocalSate();

            if (tryCoordinate()) {
                updateLocalSate();
            }

            return gossip();
        }

        return null;
    }

    public ClusterNode getNode() {
        return node;
    }

    public ClusterAddress getNodeAddress() {
        return node.getAddress();
    }

    public ClusterNodeId getId() {
        return id;
    }

    public GossipNodeStatus getStatus() {
        return status;
    }

    private JoinRequest tryJoin(boolean trySelfJoin) {
        if (trySelfJoin && seedNodesSate.isSelfJoin()) {
            // This is the first node in the cluster -> join as a single node.
            seedNodesSate = null;

            if (status == DOWN) {
                status = JOINING;

                listener.onStatusChange(DOWN, JOINING, DefaultClusterNode.NON_JOINED_ORDER, lastTopology);
            }

            GossipNodeState upState = new GossipNodeState(node, UP).order(1);

            updateLocalGossip(localGossip.update(id, upState).maxJoinOrder(upState.getOrder()));

            if (DEBUG) {
                log.debug("Joined as single node [gossip={}]", localGossip);
            }

            updateLocalSate();

            return null;
        } else {
            // Try to contact a seed node.
            InetSocketAddress target = seedNodesSate.getNextSeed();

            JoinRequest request = null;

            if (target != null) {
                request = new JoinRequest(node, cluster, target);

                if (DEBUG) {
                    log.debug("Created join request [request={}", request);
                }
            }

            return request;
        }
    }

    private Collection<UpdateBase> tryCoordinateAndGossip(int size, GossipPolicy policy) {
        assert size > 0 : "Gossip size must be above zero [size=" + size + ']';

        if (tryCoordinate()) {
            updateLocalSate();
        }

        return doGossip(size, policy);
    }

    private Collection<UpdateBase> doGossip(int size, GossipPolicy policy) {
        assert size > 0 : "Gossip size must be above zero [size=" + size + ']';

        List<GossipNodeState> nodes = localGossip.stream()
            .filter(this::canGossip)
            .collect(toList());

        if (!nodes.isEmpty()) {
            GossipNodeState fromNode = localGossip.getMember(node.getId());

            Set<ClusterNodeId> seen = localGossip.getSeen();

            Collection<GossipNodeState> selected = policy.selectNodes(size, fromNode, nodes, seen);

            if (!selected.isEmpty()) {
                List<UpdateBase> messages = new ArrayList<>(selected.size());

                GossipDigest digest = null;

                for (GossipNodeState n : selected) {
                    // Send full gossip state only to those nodes that haven't seen it yet; otherwise send digest only.
                    if (seen.contains(n.getId())) {
                        if (digest == null) {
                            digest = new GossipDigest(localGossip);
                        }

                        messages.add(new UpdateDigest(address, n.getNodeAddress(), digest));
                    } else {
                        messages.add(new Update(address, n.getNodeAddress(), localGossip));
                    }
                }

                return messages;
            }
        }

        return Collections.emptyList();
    }

    private boolean tryCoordinate() {
        if (localGossip.isConvergent()) {
            if (localGossip.isCoordinator(id)) {
                if (TRACE) {
                    log.trace("Coordinating nodes [gossip={}]", localGossip);
                }

                List<GossipNodeState> modified = new ArrayList<>();
                Set<ClusterNodeId> removed = new HashSet<>();

                AtomicInteger maxOrder = new AtomicInteger(localGossip.getMaxJoinOrder());

                // Advance nodes state.
                localGossip.stream().forEach(m -> {
                    GossipNodeStatus newStatus = null;
                    Integer order = null;

                    if (m.getStatus() == JOINING) {
                        newStatus = UP;

                        order = maxOrder.incrementAndGet();
                    } else if (m.getStatus() == LEAVING) {
                        newStatus = DOWN;
                    } else if (m.getStatus() == DOWN) {
                        // Remove DOWN node from the gossip state and put its ID to the list of removed nodes.
                        // This allows us to reduce the size of gossip messages but still allows us to track causal history.
                        if (DEBUG) {
                            log.debug("Removing {} node [node={}]", m.getStatus(), m.getNode());
                        }

                        removed.add(m.getId());
                    }

                    if (newStatus != null) {
                        if (DEBUG) {
                            log.debug("Changed node state [node={}, old={}, new={}]", m.getNode(), m.getStatus(), newStatus);
                        }

                        GossipNodeState newState = m.status(newStatus);

                        if (order != null) {
                            // Assign order to a node that switched from JOINING to UP state.
                            newState = newState.order(order);
                        }

                        modified.add(newState);
                    }
                });

                boolean changed = false;

                // Update local state if there were some state changes.
                if (!modified.isEmpty()) {
                    changed = true;

                    updateLocalGossip(localGossip.update(id, modified).maxJoinOrder(maxOrder.get()));
                }

                // Update local state if some DOWN nodes were removed.
                if (!removed.isEmpty() && !removed.equals(localGossip.getRemoved())) {
                    changed = true;

                    updateLocalGossip(localGossip.purge(id, removed));
                }

                if (changed) {
                    if (DEBUG) {
                        log.debug("Coordinated nodes [modified={}, removed={}, gossip={}]", modified, removed, localGossip);
                    }

                    return true;
                } else {
                    if (TRACE) {
                        log.trace("Coordinated nodes without any state changes.");
                    }

                    return false;
                }

            } else {
                if (DEBUG) {
                    log.debug("Local node is not a coordinator [coordinator={}]", localGossip.getCoordinator(id));
                }
            }
        }

        return false;
    }

    private boolean updateLocalSate() {
        GossipNodeState thisNode = localGossip.getMember(id);

        GossipNodeStatus newStatus = thisNode.getStatus();

        if (newStatus == status) {
            // Local node's status didn't change -> notify on topology change only.
            updateTopology(true);
        } else {
            // Local node's status was changed -> notify on status change only (since status change event also includes topology).
            GossipNodeStatus oldStatus = status;

            status = newStatus;

            if (DEBUG) {
                log.debug("Updated local node state [old={}, new={}]", oldStatus, newStatus);
            }

            updateTopology(false /* <- do not fire topology change event */);

            // Do notify on status change.
            listener.onStatusChange(oldStatus, newStatus, thisNode.getOrder(), lastTopology);
        }

        updateWatchNodes();

        if (leaveScheduled
            // Can leave if in UP state.
            && (newStatus == UP
            // Or if in JOINING state and cluster is convergent.
            || newStatus == JOINING && localGossip.isConvergent())) {
            if (DEBUG) {
                log.debug("Processing scheduled leave operation [gossip={}]", gossip());
            }

            return leave() != null;
        }

        return false;
    }

    private void updateTopology(boolean notifyOnChange) {
        Set<ClusterNode> oldTopology = lastTopology;

        Set<ClusterNode> newTopology = localGossip.getMembers().values().stream()
            .filter(s -> s.getNode().equals(node) || s.getStatus() == UP || s.getStatus() == LEAVING)
            .map(GossipNodeState::getNode)
            .collect(Collectors.toSet());

        newTopology = Collections.unmodifiableSet(newTopology);

        lastTopology = newTopology;

        if (notifyOnChange && !oldTopology.equals(newTopology)) {
            listener.onTopologyChange(oldTopology, newTopology);
        }

        updateKnownAddresses();
    }

    private void updateKnownAddresses() {
        Set<ClusterAddress> oldKnown = knownAddresses;

        Set<ClusterAddress> newKnown = localGossip.getMembers().values().stream()
            .map(GossipNodeState::getNodeAddress)
            .collect(Collectors.toSet());

        newKnown = Collections.unmodifiableSet(newKnown);

        if (!oldKnown.equals(newKnown)) {
            knownAddresses = newKnown;

            listener.onKnownAddressesChange(oldKnown, newKnown);
        }
    }

    private void updateWatchNodes() {
        Set<ClusterAddress> nodesToWatch = localGossip.stream()
            .map(GossipNodeState::getNodeAddress)
            .collect(Collectors.toSet());

        healthManager.update(nodesToWatch);
    }

    private boolean canGossip(GossipNodeState n) {
        // Node is remote and is not suspected to be failed.
        return n != null && !n.getId().equals(id) && !localGossip.isSuspected(n.getId());
    }

    private void updateLocalGossip(Gossip update) {
        localGossip = update;
    }
}
