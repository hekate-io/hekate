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

/**
 * Lifecycle callback for {@link NetworkServer}.
 *
 * @see NetworkServer#start(InetSocketAddress, NetworkServerCallback)
 */
public interface NetworkServerCallback {
    /**
     * Called if server {@link NetworkServer#start(InetSocketAddress) started} successfully.
     *
     * @param server Server.
     */
    default void onStart(NetworkServer server) {
        // No-op.
    }

    /**
     * Called if server {@link NetworkServer#stop() stopped}.
     *
     * @param server Server.
     */
    default void onStop(NetworkServer server) {
        // No-op.
    }

    /**
     * Called if there is an error either when the server is staring or already started.
     *
     * <p>
     * Implementations of this method can provide a custom failover policy.
     * For more details please see "Server failover" section of {@link NetworkServer} javadoc.
     * </p>
     *
     * @param server Server.
     * @param failure Information about the cause of this failure and failover context.
     *
     * @return Failure resolution. Returning {@code null} has the same effect as returning {@link NetworkServerFailure.Resolution#fail()}.
     */
    default NetworkServerFailure.Resolution onFailure(NetworkServer server, NetworkServerFailure failure) {
        return failure.fail();
    }
}
