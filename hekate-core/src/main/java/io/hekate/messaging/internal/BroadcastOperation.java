package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.operation.SendAckMode;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryFailure;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

import static io.hekate.messaging.retry.RetryRoutingPolicy.RETRY_SAME_NODE;

class BroadcastOperation<T> extends SendOperation<T> {
    private final ClusterNode node;

    public BroadcastOperation(
        T message,
        Object affinityKey,
        long timeout,
        int maxAttempts,
        RetryErrorPolicy retryErr,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        SendAckMode ackMode,
        ClusterNode node
    ) {
        super(
            message,
            affinityKey,
            timeout,
            maxAttempts,
            retryErr,
            retryCondition,
            retryBackoff,
            retryCallback,
            RETRY_SAME_NODE,
            gateway,
            opts,
            ackMode
        );

        this.node = node;
    }

    @Override
    public ClusterNodeId route(PartitionMapper mapper, Optional<RetryFailure> prevFailure) {
        return node.id();
    }
}
