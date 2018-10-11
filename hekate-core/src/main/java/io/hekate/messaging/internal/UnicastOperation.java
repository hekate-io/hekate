package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.partition.PartitionMapper;
import java.util.Optional;

abstract class UnicastOperation<T> extends MessageOperation<T> {
    public UnicastOperation(
        T message,
        Object affinityKey,
        MessagingGatewayContext<T> gateway,
        MessageOperationOpts<T> opts,
        boolean threadAffinity
    ) {
        super(message, affinityKey, gateway, opts, threadAffinity);
    }

    @Override
    public ClusterNodeId route(PartitionMapper mapper, Optional<FailureInfo> prevFailure) throws LoadBalancerException {
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
