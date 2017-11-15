package io.hekate.rpc;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.util.List;
import java.util.Set;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to Remote Procedure Call (RPC) API.
 *
 *
 * <h2>Overview</h2>
 * <p>
 * This service provides support for remote calls of Java objects in a cluster of {@link Hekate} nodes. Each such object must declare one
 * or more Java interfaces marked with the @{@link Rpc} annotation. Remote nodes (clients) use this interface to build a proxy object that
 * will translate all local calls of methods of this proxy object to remote calls on cluster nodes.
 * </p>
 *
 * <h2>Service configuration</h2>
 * <p>
 * {@link RpcService} can be registered and configured in {@link HekateBootstrap} with the help of {@link RpcServiceFactory} as shown in the
 * example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: rpc/RpcServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: rpc/rpc-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: rpc/rpc-bean.xml#example}
 * </div>
 * </div>
 *
 * <h2>Accessing service</h2>
 * <p>
 * {@link RpcService} can be accessed via {@link Hekate#rpc()} method as in the example below:
 * ${source: rpc/RpcServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>RPC interface</h2>
 * <p>
 * TODO
 * </p>
 * <p>
 * ${source: rpc/RpcServiceJavadocTest.java#interface}
 * </p>
 *
 * <h2>RPC server</h2>
 * <p>
 * TODO
 * </p>
 * <p>
 * ${source: rpc/RpcServiceJavadocTest.java#impl}
 * </p>
 *
 * <h2>RPC client</h2>
 * <p>
 * TODO
 * </p>
 * <p>
 * ${source: rpc/RpcServiceJavadocTest.java#client}
 * </p>
 *
 * <h2>Routing and load balancing</h2>
 * <p>
 * TODO
 * </p>
 *
 * <h2>Error handling and failover</h2>
 * <p>
 * TODO
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
     * Returns an immutable list of all RPC servers registered on this node.
     *
     * @return Immutable list of RPC servers.
     *
     * @see RpcServiceFactory#setServers(List)
     */
    List<RpcServerInfo> servers();
}
