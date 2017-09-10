package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.core.ServiceInfo;
import io.hekate.messaging.MessagingService;

class ChannelMetaData {
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
                ChannelMetaData meta = parse(service.stringProperty(propertyName(channelName)));

                if (meta != null && meta.isReceiving()) {
                    return delegate == null || delegate.accept(node);
                }
            }

            return false;
        }
    }

    private final boolean receiving;

    private final String type;

    private final int partitions;

    private final int backupNodes;

    public ChannelMetaData(boolean receiving, String type, int partitions, int backupNodes) {
        this.receiving = receiving;
        this.type = type;
        this.partitions = partitions;
        this.backupNodes = backupNodes;
    }

    public static ChannelMetaData parse(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        String[] tokens = str.split(":");

        boolean receiving = Boolean.valueOf(tokens[0]);
        String type = tokens[1];
        int partitions = Integer.parseInt(tokens[2]);
        int backupNodes = Integer.parseInt(tokens[3]);

        return new ChannelMetaData(receiving, type, partitions, backupNodes);
    }

    public static String propertyName(String channel) {
        return "channel." + channel.trim();
    }

    public static ClusterNodeFilter hasReceiver(String channelName, ClusterNodeFilter delegate) {
        return new HasReceiver(channelName, delegate);
    }

    public String type() {
        return type;
    }

    public int partitions() {
        return partitions;
    }

    public int backupNodes() {
        return backupNodes;
    }

    public boolean isReceiving() {
        return receiving;
    }

    public String toString() {
        return String.valueOf(receiving) + ':' + type + ':' + partitions + ':' + backupNodes;
    }
}
