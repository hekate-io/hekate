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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.util.format.ToString;

class DefaultMessagingEndpoint<T> implements MessagingEndpoint<T> {
    private final ClusterNodeId remoteNodeId;

    private final MessagingChannel<T> channel;

    private volatile Object userContext;

    public DefaultMessagingEndpoint(ClusterNodeId remoteNodeId, MessagingChannel<T> channel) {
        assert remoteNodeId != null : "Remote node ID is null.";
        assert channel != null : "Channel is null.";

        this.remoteNodeId = remoteNodeId;
        this.channel = channel;
    }

    @Override
    public ClusterNodeId remoteNodeId() {
        return remoteNodeId;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C> C getContext() {
        return (C)userContext;
    }

    @Override
    public void setContext(Object ctx) {
        this.userContext = ctx;
    }

    @Override
    public MessagingChannel<T> channel() {
        return channel;
    }

    @Override
    public String toString() {
        return ToString.format(MessagingEndpoint.class, this);
    }
}
