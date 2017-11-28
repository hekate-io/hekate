package foo.bar;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.rpc.RpcLoadBalancer;
import io.hekate.rpc.RpcRequest;

public class SomeRpcLoadBalancer implements RpcLoadBalancer {
    @Override
    public ClusterNodeId route(RpcRequest message, LoadBalancerContext ctx) {
        return ctx.topology().random().id();
    }
}
