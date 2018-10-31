package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.unicast.SendAckMode;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

class BroadcastOperation<T> extends SendOperation<T> {
    private final ClusterNode node;

    public BroadcastOperation(
        T message,
        Object affinityKey,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        SendAckMode ackMode,
        ClusterNode node
    ) {
        super(message, affinityKey, gateway, opts, ackMode);

        this.node = node;
    }

    @Override
    public ClusterNodeId route(PartitionMapper mapper, Optional<FailureInfo> prevFailure) {
        return node.id();
    }
}
