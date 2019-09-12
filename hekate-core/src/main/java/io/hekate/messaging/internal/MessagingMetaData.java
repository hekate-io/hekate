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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.core.ServiceInfo;
import io.hekate.messaging.MessagingService;

class MessagingMetaData {
    private static class HasReceiver implements ClusterNodeFilter {
        private final String channelName;

        private final ClusterNodeFilter delegate;

        public HasReceiver(String channelName, ClusterNodeFilter delegate) {
            assert channelName != null : "Channel name is null.";

            this.channelName = channelName;
            this.delegate = delegate;
        }

        @Override
        public boolean accept(ClusterNode node) {
            ServiceInfo service = node.service(MessagingService.class);

            if (service != null) {
                MessagingMetaData meta = parse(service.stringProperty(propertyName(channelName)));

                if (meta != null && meta.isReceiving()) {
                    return delegate == null || delegate.accept(node);
                }
            }

            return false;
        }
    }

    private final boolean receiving;

    private final String type;

    public MessagingMetaData(boolean receiving, String type) {
        this.receiving = receiving;
        this.type = type;
    }

    public static MessagingMetaData parse(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        String[] tokens = str.split(":", 2);

        boolean receiving = Boolean.valueOf(tokens[0]);
        String type = tokens[1];

        return new MessagingMetaData(receiving, type);
    }

    public static String propertyName(String channel) {
        return "channel." + channel.trim();
    }

    public static ClusterNodeFilter hasReceiver(String channelName, ClusterNodeFilter delegate) {
        return new HasReceiver(channelName, delegate);
    }

    public static ClusterNodeFilter hasReceiver(String channelName) {
        return hasReceiver(channelName, null);
    }

    public String type() {
        return type;
    }

    public boolean isReceiving() {
        return receiving;
    }

    @Override
    public String toString() {
        return String.valueOf(receiving) + ':' + type;
    }
}
