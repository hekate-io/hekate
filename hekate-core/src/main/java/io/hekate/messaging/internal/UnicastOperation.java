package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import io.hekate.messaging.retry.RetryCallback;
import io.hekate.messaging.retry.RetryCondition;
import io.hekate.messaging.retry.RetryErrorPredicate;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

abstract class UnicastOperation<T> extends MessageOperation<T> {
    public UnicastOperation(
        T message,
        Object affinityKey,
        long timeout,
        int maxAttempts,
        RetryErrorPredicate retryErr,
        RetryCondition retryCondition,
        RetryBackoffPolicy retryBackoff,
        RetryCallback retryCallback,
        RetryRoutingPolicy retryRoute,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        boolean threadAffinity
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
            retryRoute,
            gateway,
            opts,
            threadAffinity
        );
    }

    @Override
    public ClusterNodeId route(PartitionMapper mapper, Optional<FailedAttempt> prevFailure) throws LoadBalancerException {
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
