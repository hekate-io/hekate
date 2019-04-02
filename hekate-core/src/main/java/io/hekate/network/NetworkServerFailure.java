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
 * Failure in {@link NetworkServer}.
 *
 * <p>
 * Information that is provided by this interface can be used to implement server failover functionality. For more details please see
 * "Server failover" section of {@link NetworkServer} javadoc.
 * </p>
 *
 * @see NetworkServerCallback#onFailure(NetworkServer, NetworkServerFailure)
 */
public interface NetworkServerFailure {
    /**
     * Resolution of {@link NetworkServerFailure}.
     *
     * @see NetworkServerCallback#onFailure(NetworkServer, NetworkServerFailure)
     */
    interface Resolution {
        /**
         * Returns {@code true} if failure is unrecoverable and {@code false} if error is recoverable and failover actions should be
         * performed by the server.
         *
         * @return {@code true} if failure is unrecoverable and {@code false} if error is recoverable and failover actions should be
         * performed by the server.
         */
        boolean isFailure();

        /**
         * Returns the amount of time in milliseconds for server to wait before performing another failover attempt.
         *
         * @return Time in milliseconds.
         */
        long retryDelay();

        /**
         * Returns a new address that the server should try during its failover actions. Returning {@code null} instructs the server
         * to reuse the old address (see {@link NetworkServerFailure#lastTriedAddress()}).
         *
         * @return New address that the server should try during its failover actions.
         */
        InetSocketAddress retryAddress();

        /**
         * Sets the amount of time in milliseconds for server to wait before performing another failover attempt.
         *
         * @param delay Time in milliseconds.
         *
         * @return This instance.
         */
        Resolution withRetryDelay(long delay);

        /**
         * Sets the new address that server should try during its failover actions.
         *
         * @param address New address or {@code null} if server should retry the same address.
         *
         * @return This instance.
         */
        Resolution withRetryAddress(InetSocketAddress address);
    }

    /**
     * Returns the cause of this failure.
     *
     * @return Cause of this failure.
     */
    Throwable cause();

    /**
     * Returns the current failover attempt (starting with 0).
     *
     * @return Current failover attempt.
     */
    int attempt();

    /**
     * Returns the last address that was tried by the server.
     *
     * @return Last address that was tried by the server.
     */
    InetSocketAddress lastTriedAddress();

    /**
     * Resolves this failure to be unrecoverable.
     *
     * @return Failure resolution.
     */
    Resolution fail();

    /**
     * Resolves this failure to be recoverable.
     *
     * @return Failure resolution.
     */
    Resolution retry();
}
