/*
 * Copyright 2017 The Hekate Project
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
 * Callback for receiving messages and tracking state changes in {@link NetworkClient}.
 *
 * <p>
 * Instances of this interface should be passed to the {@link NetworkClient#connect(InetSocketAddress, NetworkClientCallback)} method.
 * </p>
 *
 * @param <T> Base type of messages that can be sent/received by a {@link NetworkClient}.
 *
 * @see NetworkClient
 */
@FunctionalInterface
public interface NetworkClientCallback<T> {
    /**
     * Called when a new message is received from the server.
     *
     * @param message Message.
     * @param client Client that received this message.
     *
     * @throws IOException Message handling error.
     */
    void onMessage(NetworkMessage<T> message, NetworkClient<T> client) throws IOException;

    /**
     * Called upon successful completion of {@link NetworkClient#connect(InetSocketAddress, NetworkClientCallback) connect} operation right
     * after client switches to the {@link NetworkClient.State#CONNECTED CONNECTED} state.
     *
     * <p>
     * Note that this method will not be called in case of {@link NetworkClient#connect(InetSocketAddress, NetworkClientCallback) connect}
     * operation failure, {@link #onFailure(NetworkClient, Throwable)} method will be called instead.
     * </p>
     *
     * @param client Client that was connected.
     */
    default void onConnect(NetworkClient<T> client) {
        // No-op.
    }

    /**
     * Called in case of an error in {@link NetworkClient}.
     *
     * <p>
     * If client was in {@link NetworkClient.State#CONNECTED CONNECTED} state at the time of this error then {@link
     * #onDisconnect(NetworkClient)} method will be called after this method.
     * </p>
     *
     * @param client Failed client.
     * @param error Error.
     */
    default void onFailure(NetworkClient<T> client, Throwable error) {
        // No-op.
    }

    /**
     * Called right after {@link NetworkClient} gets disconnected and switched to the {@link NetworkClient.State#DISCONNECTED DISCONNECTED}
     * state either by explicit call of {@link NetworkClient#disconnect()} method or if connection was closed due to an error.
     *
     * <p>
     * This method will not be called if {@link NetworkClient#connect(InetSocketAddress, NetworkClientCallback) connect} operation failed
     * (i.e. this method gets called only if preceding {@link #onConnect(NetworkClient)} method was called). In such case only {@link
     * #onFailure(NetworkClient, Throwable)} method will be called.
     * </p>
     *
     * @param client Client that was disconnected.
     */
    default void onDisconnect(NetworkClient<T> client) {
        // No-op.
    }
}
