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

package io.hekate.cluster;

import io.hekate.core.Hekate;
import io.hekate.core.HekateException;

/**
 * Signals that the joining node has been rejected by {@link ClusterAcceptor}.
 *
 * @see ClusterAcceptor
 */
public class ClusterRejectedJoinException extends ClusterException {
    private static final long serialVersionUID = 1;

    private final String rejectReason;

    private final ClusterAddress rejectedBy;

    /**
     * Constructs new instance with the specified reject reason and address of the node that rejected this node joining.
     *
     * @param rejectReason Reject reason as it was returned by {@link ClusterAcceptor#acceptJoin(ClusterNode, Hekate)}.
     * @param rejectedBy Address of the node that rejected this node joining.
     */
    public ClusterRejectedJoinException(String rejectReason, ClusterAddress rejectedBy) {
        super(rejectReason);

        this.rejectReason = rejectReason;
        this.rejectedBy = rejectedBy;
    }

    private ClusterRejectedJoinException(ClusterRejectedJoinException cause) {
        super(cause.getMessage(), cause);

        this.rejectReason = cause.rejectReason();
        this.rejectedBy = cause.rejectedBy();
    }

    /**
     * Returns reject reason as it was returned by {@link ClusterAcceptor#acceptJoin(ClusterNode, Hekate)}.
     *
     * @return Reject reason.
     */
    public String rejectReason() {
        return rejectReason;
    }

    /**
     * Returns address of the node that rejected this node joining.
     *
     * @return Address of the node that rejected this node joining.
     */
    public ClusterAddress rejectedBy() {
        return rejectedBy;
    }

    @Override
    public HekateException forkFromAsync() {
        return new ClusterRejectedJoinException(this);
    }
}
