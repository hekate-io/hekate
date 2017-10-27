package io.hekate.rpc;

import io.hekate.messaging.MessagingService;
import io.hekate.messaging.unicast.LoadBalancer;

/**
 * Client-side load balancer for RPC requests.
 *
 * <p>
 * This interface represents the RPC-specific version of a more generic {@link LoadBalancer} interface from the {@link MessagingService
 * messaging} API.
 * </p>
 *
 * <p>
 * Instances of this interface can be registered at configuration time via {@link RpcClientConfig#setLoadBalancer(RpcLoadBalancer)} or
 * dynamically at runtime via {@link RpcClientBuilder#withLoadBalancer(RpcLoadBalancer)}.
 * </p>
 *
 * <p>
 * For more details about load balancing please see the documentation of the {@link LoadBalancer} interface.
 * </p>
 *
 * @see RpcService
 * @see RpcRequest
 */
public interface RpcLoadBalancer extends LoadBalancer<RpcRequest> {
    // No-op.
}
