package io.hekate.rpc;

import java.util.Collection;
import java.util.List;

/**
 * Provider of RPC clients configuration.
 *
 * <p>
 * Instances of this interface are responsible for providing RPC clients configuration that were obtained from some third party sources or
 * constructed dynamically based on the provider-specific rules. Instances of this interface can be registered via {@link
 * RpcServiceFactory#setClientProviders(List)} method.
 * </p>
 *
 * <p>
 * Another approach to register RPC clients configuration is to use {@link RpcServiceFactory#setClients(List)} method.
 * </p>
 */
public interface RpcClientConfigProvider {
    /**
     * Returns a collection of RPC client configurations that should be registered within the {@link RpcService}.
     *
     * @return RPC clients configuration.
     */
    Collection<RpcClientConfig> getRpcClientConfig();
}
