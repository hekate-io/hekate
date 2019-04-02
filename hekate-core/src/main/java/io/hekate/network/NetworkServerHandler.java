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

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Handler for processing connections from {@link NetworkClient}s.
 *
 * <p>
 * This interface represents a server handler that can be registered within the {@link NetworkConnector} configuration in order to make it
 * work as a server and be able to accept connections from remote {@link NetworkClient}s.
 * </p>
 *
 * <p>
 * For more details about connectors and networking please see the documentation of {@link NetworkService}.
 * </p>
 *
 * @param <T> Base type of messages that can be sent/received by this handler.
 *
 * @see NetworkService
 * @see NetworkConnectorConfig#setServerHandler(NetworkServerHandler)
 */
@FunctionalInterface
public interface NetworkServerHandler<T> {
    /**
     * Called when new message is received from the client.
     *
     * @param msg Message.
     * @param from Client connection.
     *
     * @throws IOException Message handling error.
     */
    void onMessage(NetworkMessage<T> msg, NetworkEndpoint<T> from) throws IOException;

    /**
     * Called when {@link NetworkClient} connects to the server.
     *
     * @param login Login message that was submitted by the client via
     * {@link NetworkClient#connect(InetSocketAddress, Object, NetworkClientCallback)} method. If client haven't submitted any login
     * message then this parameter will be {@code null}.
     * @param client Client connection.
     */
    default void onConnect(T login, NetworkEndpoint<T> client) {
        // No-op.
    }

    /**
     * Called when client connection is closed.
     *
     * @param client Client connection.
     */
    default void onDisconnect(NetworkEndpoint<T> client) {
        // No-op.
    }

    /**
     * Called if error happened while communicating with the client.
     *
     * @param client Client connection.
     * @param cause Error.
     */
    default void onFailure(NetworkEndpoint<T> client, Throwable cause) {
        // No-op.
    }
}
