package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryFailure;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

abstract class UnicastOperation<T> extends MessageOperation<T> {
    public UnicastOperation(
        T message,
        Object affinityKey,
        int maxAttempts,
        RetryErrorPolicy retryErr,
        RetryCondition retryCondition,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        boolean threadAffinity
    ) {
        super(message, affinityKey, maxAttempts, retryErr, retryCondition, retryCallback, retryRoute, gateway, opts, threadAffinity);
    }

    @Override
    public ClusterNodeId route(PartitionMapper mapper, Optional<RetryFailure> prevFailure) throws LoadBalancerException {
        LoadBalancerContext ctx = new DefaultLoadBalancerContext(
            affinity(),
            affinityKey(),
            mapper.topology(),
            mapper,
            prevFailure
        );

        return opts().balancer().route(message(), ctx);
    }
}
