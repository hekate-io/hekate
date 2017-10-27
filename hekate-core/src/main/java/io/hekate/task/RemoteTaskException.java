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

package io.hekate.task;

import io.hekate.cluster.ClusterNodeId;

/**
 * Signals that there was a task execution failure on a remote node.
 *
 * @see #remoteNodeId()
 * @see TaskService
 */
public class RemoteTaskException extends TaskException {
    private static final long serialVersionUID = 1;

    private final ClusterNodeId remoteNodeId;

    /**
     * Constructs a new instance.
     *
     * @param remoteNodeId Remote node.
     * @param cause Error cause.
     */
    public RemoteTaskException(ClusterNodeId remoteNodeId, Throwable cause) {
        super("Remote task execution failed [node-id=" + remoteNodeId + ']', cause);

        this.remoteNodeId = remoteNodeId;
    }

    /**
     * Returns the remote node of this failure.
     *
     * @return Remote node.
     */
    public ClusterNodeId remoteNodeId() {
        return remoteNodeId;
    }
}
