package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryFailure;
import io.hekate.messaging.retry.RetryResponsePolicy;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

import static io.hekate.messaging.retry.RetryRoutingPolicy.RETRY_SAME_NODE;

class AggregateOperation<T> extends RequestOperation<T> {
    private final ClusterNode node;

    public AggregateOperation(
        T message,
        Object affinityKey,
        long timeout,
        int maxAttempts,
        RetryErrorPolicy retryErr,
        RetryResponsePolicy<T> retryRsp,
        RetryCondition retryCondition,
        RetryCallback retryCallback,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        ClusterNode node
    ) {
        super(
            message,
            affinityKey,
            timeout,
            maxAttempts,
            retryErr,
            retryRsp,
            retryCondition,
            retryCallback,
            RETRY_SAME_NODE,
            gateway,
            opts
        );

        this.node = node;
    }

    @Override
    public ClusterNodeId route(PartitionMapper mapper, Optional<RetryFailure> prevFailure) {
        return node.id();
    }
}
