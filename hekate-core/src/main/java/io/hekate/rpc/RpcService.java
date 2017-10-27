package io.hekate.rpc;

import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.util.List;
import java.util.Set;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to Remote Procedure Call (RPC) API.
 *
 * <p>
 * TODO: More documentation for RpcService.
 * </p>
 */
@DefaultServiceFactory(RpcServiceFactory.class)
public interface RpcService extends Service {
    /**
     * Constructs a new RPC client proxy builder for the specified Java interface and tag.
     *
     * <p>
     * This method returns an instance of {@link RpcClientBuilder} that can be used to configure and {@link RpcClientBuilder#build() build}
     * a Java proxy object for the specified RPC interface that will redirect all method invocations to remote cluster nodes.
     * </p>
     *
     * <p>
     * <b>Note:</b> Some of the builder's options can be preconfigured via {@link RpcClientConfig} (see its javadocs for more details).
     * </p>
     *
     * @param type RPC interface (must be an @{@link Rpc}-annotated Java interface).
     * @param tag Tag (see {@link RpcServerConfig#setTags(Set)}).
     * @param <T> RPC interface.
     *
     * @return Builder.
     */
    <T> RpcClientBuilder<T> clientFor(Class<T> type, String tag);

    /**
     * Constructs a new RPC client proxy builder for the specified Java interface.
     *
     * <p>
     * This method returns an instance of {@link RpcClientBuilder} that can be used to configure and {@link RpcClientBuilder#build() build}
     * a Java proxy object for the specified RPC interface that will redirect all method invocations to remote cluster nodes.
     * </p>
     *
     * <p>
     * <b>Note:</b> Some of the builder's options can be preconfigured via {@link RpcClientConfig} (see its javadocs for more details).
     * </p>
     *
     * @param type RPC interface (must be an @{@link Rpc}-annotated Java interface).
     * @param <T> RPC interface.
     *
     * @return Builder.
     */
    <T> RpcClientBuilder<T> clientFor(Class<T> type);

    /**
     * Returns an immutable list of all registered RPC servers.
     *
     * @return Immutable list of RPC servers.
     *
     * @see RpcServiceFactory#setServers(List)
     */
    List<RpcServerInfo> servers();
}
