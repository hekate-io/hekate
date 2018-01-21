/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.network.netty;

import io.hekate.network.NetworkServer;

/**
 * Netty-based extension of {@link NetworkServer} interface.
 *
 * @see NettyServerFactory
 */
public interface NettyServer extends NetworkServer {
    /**
     * Sets the metrics factory to be used by this server.
     *
     * <p>
     * This method can be used to dynamically re-configure metrics factory that is used by this server. Passing a {@code null} value to
     * this method disables metrics gathering by this server. Setting a not-{@code null} value will apply the specified metrics factory to
     * all new connections that are established to this server (i.e. existing connections are not affected by this method).
     * </p>
     *
     * @param metrics Metrics factory or {@code null} (disables metrics gathering).
     *
     * @see NettyServerFactory#setMetrics(NettyMetricsFactory)
     */
    void setMetrics(NettyMetricsFactory metrics);
}
