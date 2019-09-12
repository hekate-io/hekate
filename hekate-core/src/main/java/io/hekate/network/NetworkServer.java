/*
 * Copyright 2019 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.network;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Standalone TCP server.
 *
 * <h2>Overview</h2>
 * <p>
 * This interface represents a standalone TCP server.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * Configuration of the standalone TCP server is represented by {@link NetworkServerFactoryBase} class. Please see its javadoc for
 * information about available configuration options.
 * </p>
 *
 * <h2>Starting and Stopping</h2>
 * <p>
 * Server can be started by calling {@link #start(InetSocketAddress, NetworkServerCallback)} method and providing an optional callback that
 * will be notified upon lifecycle events. This method is asynchronous and returns a future object that can be used to get operation
 * results.
 * </p>
 *
 * <p>
 * It is possible to start server that will bind to a network interface but will not automatically accept connections from
 * {@link NetworkClient}s. This can be achieved by setting {@link NetworkServerFactoryBase#setAutoAccept(boolean)} option to {@code false}.
 * In this case all clients that are trying to connect will be blocked unless {@link #startAccepting()} method is called.
 * </p>
 *
 * <p>
 * Server can be stopped by calling {@link #stop()} method. This method is also asynchronous and returns a future object that can be used
 * to get operation results.
 * </p>
 *
 * <h2>Instantiation</h2>
 * <p>
 * Instances of this interface can be obtained via {@link NetworkServerFactoryBase#createServer()} method.
 * </p>
 *
 * <h2>Server Failover</h2>
 * <p>
 * Whenever an error occurs, server notifies {@link NetworkServerCallback#onFailure(NetworkServer, NetworkServerFailure)} callback.
 * Implementation of this method can control which actions should be performed in this case by analyzing {@link NetworkServerFailure} and
 * returning a {@link NetworkServerFailure.Resolution} instance. Based on this information server can decide on whether to try the same
 * address, rebind to another address or stop.
 * </p>
 */
public interface NetworkServer {
    /**
     * State of {@link NetworkServer} lifecycle.
     */
    enum State {
        /**
         * Server is stopped.
         */
        STOPPED,

        /**
         * Server is starting.
         */
        STARTING,

        /**
         * Server is started.
         */
        STARTED,

        /**
         * Server is stopping.
         */
        STOPPING
    }

    /**
     * Returns bind address or {@code null} if server is not started.
     *
     * @return Server address or {@code null} if server is not started.
     */
    InetSocketAddress address();

    /**
     * Returns the lifecycle state of this server.
     *
     * @return Lifecycle state.
     */
    State state();

    /**
     * Asynchronously starts this server and binds it to the specified address.
     *
     * @param bindAddress Bind address (use {@link InetSocketAddress#InetSocketAddress(int)} if server must accept connections on all
     * network interfaces). If port value of the specified address is 0 then port will be auto-assigned by the operating system.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @throws IllegalStateException Thrown if server is not in {@link NetworkServer.State#STOPPED STOPPED} state.
     */
    NetworkServerFuture start(InetSocketAddress bindAddress) throws IllegalStateException;

    /**
     * Asynchronously starts this server and binds it to the specified address.
     *
     * @param bindAddress Bind address (use {@link InetSocketAddress#InetSocketAddress(int)} if server must accept connections on all
     * network interfaces). If port value of the specified address is 0 then port will be auto-assigned by the operating system.
     * @param callback Optional callback for listening server lifecycle events and performing errors failover.
     *
     * @return Future object that can be used to obtain result of this operation.
     *
     * @throws IllegalStateException Thrown if server is not in {@link NetworkServer.State#STOPPED STOPPED} state.
     */
    NetworkServerFuture start(InetSocketAddress bindAddress, NetworkServerCallback callback);

    /**
     * Starts accepting connection from clients.
     *
     * <p>
     * This method must be called only if {@link NetworkServerFactoryBase#setAutoAccept(boolean)} is set to {@code false}.
     * If it is set to {@code true} then server will accept connections automatically once it is {@link #start(InetSocketAddress) started}.
     * </p>
     */
    void startAccepting();

    /**
     * Asynchronously stops this server and closes all active client connections.
     *
     * <p>
     * This method does nothing if server is already in {@link NetworkServer.State#STOPPED STOPPED} or {@link
     * NetworkServer.State#STOPPING STOPPING} state.
     * </p>
     *
     * @return Future object that can be used to obtain result of this operation.
     */
    NetworkServerFuture stop();

    /**
     * Registers the specified handler to this server.
     *
     * <p>
     * Handlers can be registered at any time and do not depend on whether server is stopped or is running.
     * </p>
     *
     * @param handler Handler.
     */
    void addHandler(NetworkServerHandlerConfig<?> handler);

    /**
     * Unregisters {@link NetworkServerHandler} with the specified {@link NetworkServerHandlerConfig#setProtocol(String) protocol
     * identifier} and returns a list of {@link NetworkEndpoint}s that were connected at the time of this operation.
     *
     * <p>
     * All new client connections for the specified protocol will be rejected after this operation completes.
     * </p>
     *
     * <p>
     * This method does nothing if there is no such server handler with the specified protocol identifier. In such case an empty list will
     * be returned.
     * </p>
     *
     * @param protocol Protocol identifier (see {@link NetworkServerHandlerConfig#setProtocol(String)}).
     *
     * @return List of connected clients or an empty list if there is no such handler or no connected clients.
     */
    List<NetworkEndpoint<?>> removeHandler(String protocol);

    /**
     * Returns the list of network endpoints that are currently connected to this server with the specified protocol.
     *
     * @param protocol Protocol (see {@link NetworkServerHandlerConfig#setProtocol(String)}).
     *
     * @return List of network endpoints or an empty list if there are now connected endpoints.
     */
    List<NetworkEndpoint<?>> clients(String protocol);
}
