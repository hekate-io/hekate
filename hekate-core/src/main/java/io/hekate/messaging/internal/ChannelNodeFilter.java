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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.core.ServiceInfo;
import io.hekate.messaging.MessagingService;

class ChannelNodeFilter implements ClusterNodeFilter {
    private final String channelName;

    private final ClusterNodeFilter delegate;

    public ChannelNodeFilter(String channelName, ClusterNodeFilter delegate) {
        assert channelName != null : "Channel name is null.";

        this.channelName = channelName;
        this.delegate = delegate;
    }

    public static String serviceProperty(String channel) {
        return "channel." + channel;
    }

    @Override
    public boolean accept(ClusterNode node) {
        ServiceInfo service = node.service(MessagingService.class);

        if (service != null) {
            if (service.property(serviceProperty(channelName)) != null) {
                return delegate == null || delegate.accept(node);
            }
        }

        return false;
    }
}
