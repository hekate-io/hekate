package io.hekate.rpc;

import java.util.Collection;
import java.util.List;

/**
 * Provider of RPC servers configuration.
 *
 * <p>
 * Instances of this interface are responsible for providing RPC servers configuration that were obtained from some third party sources or
 * constructed dynamically based on the provider-specific rules. Instances of this interface can be registered via {@link
 * RpcServiceFactory#setServerProviders(List)} method.
 * </p>
 *
 * <p>
 * Another approach to register RPC servers configuration is to use {@link RpcServiceFactory#setServers(List)} method.
 * </p>
 */
public interface RpcServerConfigProvider {
    /**
     * Returns a collection of RPC server configurations that should be registered within the {@link RpcService}.
     *
     * @return RPC servers configuration.
     */
    Collection<RpcServerConfig> getRpcServerConfig();
}
