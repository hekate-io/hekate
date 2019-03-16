package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
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
        RetryErrorPredicate retryErr,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        AckMode ackMode,
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
    public ClusterNodeId route(PartitionMapper mapper, Optional<FailedAttempt> prevFailure) {
        return node.id();
    }
}
