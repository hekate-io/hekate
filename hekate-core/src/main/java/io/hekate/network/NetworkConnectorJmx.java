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

import io.hekate.core.jmx.JmxTypeName;
import javax.management.MXBean;

/**
 * JMX interface for {@link NetworkConnector}.
 */
@MXBean
@JmxTypeName("NetworkConnector")
public interface NetworkConnectorJmx {
    /**
     * Returns the value of {@link NetworkConnectorConfig#setProtocol(String)}.
     *
     * @return Value of {@link NetworkConnectorConfig#setProtocol(String)}.
     */
    String getProtocol();

    /**
     * Returns the value of {@link NetworkConnectorConfig#setIdleSocketTimeout(long)}.
     *
     * @return Value of {@link NetworkConnectorConfig#setIdleSocketTimeout(long)}.
     */
    long getIdleSocketTimeout();

    /**
     * Returns the value of {@link NetworkConnectorConfig#setNioThreads(int)}.
     *
     * @return Value of {@link NetworkConnectorConfig#setNioThreads(int)}.
     */
    int getNioThreads();

    /**
     * Returns {@code true} if this connector has a server handler
     * (see {@link NetworkConnectorConfig#setServerHandler(NetworkServerHandler)}).
     *
     * @return {@code true} if this connector has a server handler.
     */
    boolean isServer();

    /**
     * Returns the value of {@link NetworkConnectorConfig#setLogCategory(String)}.
     *
     * @return Value of {@link NetworkConnectorConfig#setLogCategory(String)}.
     */
    String getLogCategory();
}
