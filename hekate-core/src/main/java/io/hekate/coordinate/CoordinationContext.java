/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.coordinate;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.HekateSupport;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * Coordination context for {@link CoordinationHandler}.
 */
public interface CoordinationContext extends HekateSupport {
    /**
     * Returns {@code true} if local node is the coordinator.
     *
     * @return {@code true} if local node is the coordinator.
     */
    boolean isCoordinator();

    /**
     * Returns the coordinator.
     *
     * @return Coordinator.
     */
    CoordinationMember coordinator();

    /**
     * Returns the cluster topology of this coordination process.
     *
     * @return Cluster topology.
     */
    ClusterTopology topology();

    /**
     * Returns {@code true} if this coordination process is complete (either {@link CoordinatorContext#complete() successfully}
     * or by {@link CoordinationHandler#cancel(CoordinationContext)} cancellation).
     *
     * @return {@code true} if this coordination process is complete.
     */
    boolean isDone();

    /**
     * Returns {@code true} if this coordination process was {@link CoordinationHandler#cancel(CoordinationContext) cancelled}.
     *
     * @return {@code true} if this coordination process was {@link CoordinationHandler#cancel(CoordinationContext) cancelled}.
     */
    boolean isCancelled();

    /**
     * Returns the local node member.
     *
     * @return Local node.
     */
    CoordinationMember localMember();

    /**
     * Returns all members of this coordination process.
     *
     * @return Coordination members.
     */
    List<CoordinationMember> members();

    /**
     * Returns member by its cluster node.
     *
     * @param node Cluster node.
     *
     * @return Member or {@code null} if there is no such member.
     */
    CoordinationMember memberOf(ClusterNode node);

    /**
     * Returns member by its cluster node identifier.
     *
     * @param nodeId Cluster node identifier.
     *
     * @return Member or {@code null} if there is no such member.
     */
    CoordinationMember memberOf(ClusterNodeId nodeId);

    /**
     * Returns the size of {@link #members()}.
     *
     * @return Size of {@link #members()}.
     */
    int size();

    /**
     * Asynchronously sends the specified request to all {@link #members() members} of this coordination process.
     *
     * <p>
     * <b>Note:</b> Request will be sent to all members including the local node.
     * </p>
     *
     * @param request Request.
     * @param callback Callback to be notified once responses have been received from all members.
     *
     * @see CoordinationHandler#process(CoordinationRequest, CoordinationContext)
     */
    void broadcast(
        Object request,
        CoordinationBroadcastCallback callback
    );

    /**
     * Asynchronously sends the specified request to those members that do match the specified filter.
     *
     * <p>
     * If the filtered members list is empty then the message will not be submitted and the specified callback will
     * {@link CoordinationBroadcastCallback#onResponses(Map) complete} with an empty result <b>on the caller thread</b>.
     * </p>
     *
     * @param request Request.
     * @param filter Filter.
     * @param callback Callback to be notified once responses have been received from all members.
     *
     * @see CoordinationHandler#process(CoordinationRequest, CoordinationContext)
     */
    void broadcast(
        Object request,
        Predicate<CoordinationMember> filter,
        CoordinationBroadcastCallback callback
    );

    /**
     * Asynchronously sends the specified request to those members that do match the specified filter and only if the specified
     * precondition is {@code true}.
     *
     * <p>
     * If precondition is {@code false} or if the filtered members list is empty then the message will not be submitted and the specified
     * callback will {@link CoordinationBroadcastCallback#onResponses(Map) complete} with an empty result <b>on the caller thread</b>.
     * </p>
     *
     * @param request Request.
     * @param preCondition Precondition.
     * @param filter Filter.
     * @param callback Callback to be notified once responses have been received from all members.
     *
     * @see CoordinationHandler#process(CoordinationRequest, CoordinationContext)
     */
    void broadcast(
        Object request,
        BooleanSupplier preCondition,
        Predicate<CoordinationMember> filter,
        CoordinationBroadcastCallback callback
    );

    /**
     * Returns a user-defined object that is attached to this context (see {@link #setAttachment(Object)}).
     *
     * @return Attachment.
     */
    Object getAttachment();

    /**
     * Sets the user-defined object that should be attached to this context.
     *
     * @param attachment Attachment.
     */
    void setAttachment(Object attachment);
}
