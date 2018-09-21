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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.health.FailureDetector;
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

    private final ClusterNode localNode;

    private final ClusterAddress address;

    private final ClusterNodeId id;

    private final FailureDetector failureDetector;

    private final GossipNodesDeathWatch deathWatch;

    private final GossipListener listener;

    private final int speedUpSize;

    private GossipNodeStatus status;

    private Gossip localGossip;

    private GossipSeedNodesSate seedNodesSate;

    private Set<ClusterNode> lastTopology = Collections.emptySet();

    private GossipNodeStatus lastStatus;

    private Set<ClusterAddress> knownAddresses = Collections.emptySet();

    private boolean leaveScheduled;

    public GossipManager(
        String cluster,
        ClusterNode localNode,
        int speedUpSize,
        FailureDetector failureDetector,
        GossipListener gossipListener
    ) {
        assert cluster != null : "Cluster is null.";
        assert localNode != null : "Local node is null.";
        assert failureDetector != null : "Health manager is null.";
        assert gossipListener != null : "Listener is null.";

        this.cluster = cluster;
        this.localNode = localNode;
        this.address = localNode.address();
        this.failureDetector = failureDetector;
        this.listener = gossipListener;
        this.speedUpSize = speedUpSize;

        id = localNode.id();

        status = DOWN;
        lastStatus = DOWN;

        updateLocalGossip(new Gossip());

        int failureQuorum = Math.max(1, failureDetector.failureQuorum());

        deathWatch = new GossipNodesDeathWatch(id, failureQuorum, 0);
    }

    public Gossip localGossip() {
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
            seedNodesSate = new GossipSeedNodesSate(address.socket(), seedNodes);
        } else {
            seedNodesSate.update(seedNodes);
        }

        return tryJoin(true);
    }

    public JoinRequest processJoinReject(JoinReject msg) {
        assert msg != null : "Message is null.";
        assert msg.to().equals(address) : "Message is addressed to another node [message=" + msg + ']';

        if (localGossip.hasMembers()) {
            if (DEBUG) {
                log.debug("Skipped join reject since local node is already joined [reply={}, gossip={}]", msg, localGossip);
            }

            return null;
        }

        if (DEBUG) {
            log.debug("Processing a join reject message [message={}]", msg);
        }

        switch (msg.rejectType()) {
            case TEMPORARY: {
                seedNodesSate.onReject(msg.rejectedAddress());

                return tryJoin(false);
            }
            case PERMANENT: {
                seedNodesSate.onBan(msg.rejectedAddress());

                return tryJoin(false);
            }
            case FATAL: {
                listener.onJoinReject(msg.from(), msg.reason());

                return null;
            }
            case CONFLICT: {
                listener.onNodeInconsistency(status);

                return null;
            }
            default: {
                throw new IllegalArgumentException("Unexpected reject type: " + msg.rejectType());
            }
        }
    }

    public JoinRequest processJoinFailure(JoinRequest msg, Throwable cause) {
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

        seedNodesSate.onFailure(msg.toAddress(), cause);

        return tryJoin(false);
    }

    public Update processJoinAccept(JoinAccept msg) {
        assert msg != null : "Message is null.";
        assert msg.to().equals(address) : "Message is addressed to another node [message=" + msg + ']';

        Gossip remote = msg.gossip();

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

            Update response = new Update(localNode.address(), msg.from(), localGossip);

            if (DEBUG) {
                log.debug("Created gossip update [update={}]", response);
            }

            return response;
        }

        return null;
    }

    public JoinReject acceptJoinRequest(JoinRequest msg) {
        assert msg != null : "Message is null.";

        if (!cluster.equals(msg.cluster())) {
            if (DEBUG) {
                log.debug("Permanently rejected join request since this node belongs to another cluster [request={}]", msg);
            }

            return JoinReject.permanent(address, msg.from(), msg.toAddress());
        } else if (msg.fromNode().id().equals(id)) {
            if (DEBUG) {
                log.debug("Permanently rejected join request from self [request={}]", msg);
            }

            return JoinReject.permanent(address, msg.from(), msg.toAddress());
        } else if (!localGossip.hasMembers()) {
            if (DEBUG) {
                log.debug("Rejected join request since local node is not joined yet [request={}]", msg);
            }

            return JoinReject.retryLater(address, msg.from(), msg.toAddress());
        } else if (localGossip.isDown(msg.fromNode().id())) {
            if (DEBUG) {
                log.debug("Rejected join request since the joining node is in inconsistent state [request={}]", msg);
            }

            return JoinReject.conflict(address, msg.from(), msg.toAddress());
        } else if (leaveScheduled || status == LEAVING || status == DOWN) {
            if (DEBUG) {
                if (leaveScheduled) {
                    log.debug("Rejected join request since local node is scheduled to leave the cluster [request={}]", msg);
                } else {
                    log.debug("Rejected join request since local node is in {} state [request={}]", status, msg);
                }
            }

            // Do not reject permanently since another node can be started on the same address once this node is stopped.
            return JoinReject.retryLater(address, msg.from(), msg.toAddress());
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

        ClusterNode joining = msg.fromNode();

        if (!localGossip.hasMember(joining.id())) {
            GossipNodeState newMember = new GossipNodeState(joining, JOINING);

            updateLocalGossip(localGossip.update(id, newMember));

            updateWatchNodes();
        }

        JoinAccept accept = new JoinAccept(address, joining.address(), localGossip);

        if (DEBUG) {
            log.debug("Created join accept reply [reply={}]", accept);
        }

        return accept;
    }

    public JoinReject reject(JoinRequest msg, String reason) {
        return JoinReject.fatal(address, msg.from(), msg.toAddress(), reason);
    }

    public Collection<UpdateBase> batchGossip(GossipPolicy policy) {
        int batchSize;

        if (localGossip.seen().size() < localGossip.members().size()) {
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
        assert msg.to().equals(address) : "Message is addressed to another node [message=" + msg + ']';

        if (status == DOWN) {
            if (DEBUG) {
                log.debug("Skipped gossip message since local node is in {} state [message={}]", DOWN, msg);
            }

            return null;
        }

        // Check gossip state version.
        long thisVer = localGossip.version();
        long otherVer = msg.gossipBase().version();

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
                return new UpdateDigest(address, msg.from(), new GossipDigest(localGossip));
            }
        }

        GossipBase remote = msg.gossipBase();

        if (!remote.hasMember(id)) {
            // Check if local node wasn't remove from the cluster.
            // Can happen in case of a long GC pause on this node.
            // Such pause can make other members to think that this node is dead and remove it from cluster.
            if (remote.removed().contains(id)) {
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
                    updateLocalGossip(update.gossip().inheritSeen(id, localGossip));

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

                if (!localGossip.hasSeenAll(remote.seen())) {
                    updateLocalGossip(localGossip.seen(remote.seen()));

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
                    Gossip merged = localGossip.merge(id, update.gossip());

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
        if (!leaveScheduled && localGossip.member(id).status() == DOWN) {
            if (DEBUG) {
                log.debug("Notifying listener on inconsistency since local node is seen as {} by remote node "
                    + "[remote-node={}]", DOWN, msg.from());
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
                reply = new UpdateDigest(address, msg.from(), new GossipDigest(localGossip));
            } else {
                reply = new Update(address, msg.from(), localGossip);
            }

            if (DEBUG) {
                log.debug("Created gossip reply [reply={}]", reply);
            }

            return reply;
        }

        // Check if speed up should be applied.
        if (status != DOWN && localGossip.members().size() <= speedUpSize) {
            UpdateBase speedUp = trySpeedUp(seenChanged);

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
                log.trace("Checking nodes aliveness using {}", failureDetector);
            }

            GossipNodeState localNodeState = localGossip.member(id);

            Set<ClusterNodeId> oldSuspects = localNodeState.suspected();

            Set<ClusterNodeId> newSuspects = new HashSet<>();

            localGossip.stream().forEach(m -> {
                ClusterNodeId nodeId = m.id();

                if (!id.equals(nodeId)) {
                    boolean isAlive = failureDetector.isAlive(m.address());

                    if (!isAlive) {
                        if (!oldSuspects.contains(nodeId)
                            // It is possible that failed node became DOWN and stopped before this node received this information.
                            && m.status() != DOWN && m.status() != LEAVING) {
                            listener.onNodeFailureSuspected(m.node(), m.status());
                        }

                        newSuspects.add(nodeId);
                    } else if (oldSuspects.contains(nodeId) && m.status() != DOWN) {
                        listener.onNodeFailureUnsuspected(m.node(), m.status());
                    }
                }
            });

            if (!newSuspects.equals(oldSuspects)) {
                if (DEBUG) {
                    log.debug("Updating local suspects table [new={}, old={}]", newSuspects, oldSuspects);
                }

                suspectsChanged = true;

                updateLocalGossip(localGossip.update(id, localNodeState.suspect(newSuspects)));
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
                            GossipNodeState member = localGossip.member(terminatedId);

                            return member != null && member.status() != DOWN;
                        })
                        .map(terminatedId -> {
                            GossipNodeState from = localGossip.member(terminatedId);

                            GossipNodeState to = from.status(DOWN);

                            if (DEBUG) {
                                log.debug("Terminating node [node={}, state={}]", from.node(), from.status());
                            }

                            listener.onNodeFailure(from.node(), from.status());

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
        if (status == LEAVING || status == DOWN) {
            if (DEBUG) {
                log.debug("Skipped leaving since local node is in {} state [gossip={}]", status, localGossip);
            }
        } else {
            leaveScheduled = true;

            if (localGossip.isConvergent()) {
                GossipNodeState newState = localGossip.member(id).status(LEAVING);

                updateLocalGossip(localGossip.update(id, newState));

                if (DEBUG) {
                    log.debug("Leaving cluster [gossip={}]", localGossip);
                }

                updateLocalSate();

                if (tryCoordinate()) {
                    updateLocalSate();
                }
            } else {
                if (DEBUG) {
                    log.debug("Scheduled leave operation to be executed once gossip reaches its convergent sate [gossip={}]", localGossip);
                }
            }

            return gossip();
        }

        return null;
    }

    public ClusterNode node() {
        return localNode;
    }

    public ClusterAddress address() {
        return localNode.address();
    }

    public ClusterNodeId id() {
        return id;
    }

    public GossipNodeStatus status() {
        return status;
    }

    private JoinRequest tryJoin(boolean trySelfJoin) {
        if (trySelfJoin && seedNodesSate.isSelfJoin()) {
            // This is the first node in the cluster -> join as a single node.
            seedNodesSate = null;

            if (status == DOWN) {
                localGossip = localGossip.update(id, new GossipNodeState(localNode, JOINING));

                updateLocalSate();
            }

            GossipNodeState upState = new GossipNodeState(localNode, UP).order(1);

            updateLocalGossip(localGossip.update(id, upState).maxJoinOrder(upState.order()));

            if (DEBUG) {
                log.debug("Joined as single node [gossip={}]", localGossip);
            }

            updateLocalSate();

            return null;
        } else {
            // Try to contact a seed node.
            InetSocketAddress target = seedNodesSate.nextSeed();

            JoinRequest request = null;

            if (target != null) {
                request = new JoinRequest(localNode, cluster, target);

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
            GossipNodeState fromNode = localGossip.member(localNode.id());

            Set<ClusterNodeId> seen = localGossip.seen();

            Collection<GossipNodeState> selected = policy.selectNodes(size, fromNode, nodes, seen);

            if (!selected.isEmpty()) {
                List<UpdateBase> messages = new ArrayList<>(selected.size());

                GossipDigest digest = null;

                for (GossipNodeState n : selected) {
                    // Send full gossip state only to those nodes that haven't seen it yet; otherwise send digest only.
                    if (seen.contains(n.id())) {
                        if (digest == null) {
                            digest = new GossipDigest(localGossip);
                        }

                        messages.add(new UpdateDigest(address, n.address(), digest));
                    } else {
                        messages.add(new Update(address, n.address(), localGossip));
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

                AtomicInteger maxOrder = new AtomicInteger(localGossip.maxJoinOrder());

                // Advance nodes state.
                localGossip.stream().forEach(m -> {
                    GossipNodeStatus newStatus = null;
                    Integer order = null;

                    if (m.status() == JOINING) {
                        newStatus = UP;

                        order = maxOrder.incrementAndGet();
                    } else if (m.status() == LEAVING) {
                        newStatus = DOWN;
                    } else if (m.status() == DOWN) {
                        // Remove DOWN node from the gossip state and put its ID to the list of removed nodes.
                        // This allows us to reduce the size of gossip messages but still allows us to track causal history.
                        if (DEBUG) {
                            log.debug("Removing {} node [node={}]", m.status(), m.node());
                        }

                        removed.add(m.id());
                    }

                    if (newStatus != null) {
                        if (DEBUG) {
                            log.debug("Changed node state [node={}, old={}, new={}]", m.node(), m.status(), newStatus);
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
                if (!removed.isEmpty() && !removed.equals(localGossip.removed())) {
                    changed = true;

                    updateLocalGossip(localGossip.purge(id, removed));
                }

                if (changed) {
                    if (DEBUG) {
                        log.debug("Coordinated nodes [modified={}, removed={}, gossip={}]", modified, removed, localGossip);
                    }
                } else {
                    if (TRACE) {
                        log.trace("Coordinated nodes without any state changes.");
                    }
                }

                return changed;
            } else {
                if (DEBUG) {
                    log.debug("Local node is not a coordinator [coordinator={}]", localGossip.coordinator(id));
                }
            }
        }

        return false;
    }

    private UpdateBase trySpeedUp(boolean seenChanged) {
        UpdateBase speedUp = null;

        // Select speed up coordinator node.
        ClusterNode coordinator = localGossip.coordinator(id);

        if (coordinator != null) {
            if (coordinator.equals(localNode)) {
                // Local node is the coordinator.
                // Try gossip to any NON DOWN node that haven't seen this state.
                speedUp = doGossip(1, GossipPolicy.RANDOM_UNSEEN_NON_DOWN).stream().findFirst().orElse(null);

                if (speedUp == null) {
                    // Try gossip to any DOWN node that haven't seen this state.
                    speedUp = doGossip(1, GossipPolicy.RANDOM_UNSEEN).stream().findFirst().orElse(null);
                }
            } else {
                // Remote node is the coordinator.
                if (!localGossip.hasSeen(coordinator.id())) {
                    // Send update directly to coordinator if he hasn't seen this gossip version.
                    speedUp = new Update(address, coordinator.address(), localGossip);
                } else if (seenChanged) {
                    // Send gossip digest directly to coordinator if seen list is known to be different from the coordinator's.
                    speedUp = new UpdateDigest(address, coordinator.address(), new GossipDigest(localGossip));
                }
            }
        }

        return speedUp;
    }

    private boolean updateLocalSate() {
        GossipNodeStatus newStatus = localGossip.member(id).status();

        if (newStatus != status) {
            if (DEBUG) {
                log.debug("Updated local node state [old={}, new={}]", status, newStatus);
            }

            status = newStatus;
        }

        updateTopology();

        if (leaveScheduled
            // Can leave only if in convergent state (otherwise node can join/leave unnoticed by some nodes).
            && localGossip.isConvergent()
            // ...and only if in JOINING or UP state.
            && (newStatus == JOINING || newStatus == UP)) {
            if (DEBUG) {
                log.debug("Processing scheduled leave operation [gossip={}]", localGossip);
            }

            return leave() != null;
        }

        return false;
    }

    private void updateTopology() {
        Set<ClusterNode> newTopology = localGossip.members().values().stream()
            .filter(s -> s.node().equals(localNode) || s.status() == UP)
            .map(GossipNodeState::node)
            .collect(Collectors.toSet());

        newTopology = Collections.unmodifiableSet(newTopology);

        Set<ClusterNode> oldTopology = lastTopology;

        GossipNodeState thisNode = localGossip.member(id);

        GossipNodeStatus newStatus = thisNode.status();
        GossipNodeStatus oldStatus = this.lastStatus;

        if (oldStatus != newStatus) {
            lastStatus = newStatus;
            lastTopology = newTopology;

            listener.onStatusChange(oldStatus, newStatus, thisNode.order(), newTopology);
        } else if (!oldTopology.equals(newTopology)) {
            lastTopology = newTopology;

            listener.onTopologyChange(oldTopology, newTopology);
        }

        updateKnownAddresses();

        updateWatchNodes();
    }

    private void updateKnownAddresses() {
        Set<ClusterAddress> oldKnown = knownAddresses;

        Set<ClusterAddress> newKnown = localGossip.members().values().stream()
            .map(GossipNodeState::address)
            .collect(Collectors.toSet());

        newKnown = Collections.unmodifiableSet(newKnown);

        if (!oldKnown.equals(newKnown)) {
            knownAddresses = newKnown;

            listener.onKnownAddressesChange(oldKnown, newKnown);
        }
    }

    private void updateWatchNodes() {
        Set<ClusterAddress> nodesToWatch = localGossip.stream()
            .map(GossipNodeState::address)
            .collect(Collectors.toSet());

        failureDetector.update(nodesToWatch);
    }

    private boolean canGossip(GossipNodeState n) {
        // Node is not a local node and is not suspected to be failed.
        return n != null && !n.id().equals(id) && !localGossip.isSuspected(n.id());
    }

    private void updateLocalGossip(Gossip update) {
        localGossip = update;
    }
}
