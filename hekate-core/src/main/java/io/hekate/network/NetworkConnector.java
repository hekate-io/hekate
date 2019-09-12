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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;

/**
 * Network connector.
 *
 * <p>
 * Instances of this interface can be obtained via {@link NetworkService#connector(String)} method.
 * </p>
 *
 * <p>
 * For more details about connectors and networking please see the documentation of {@link NetworkService}.
 * </p>
 *
 * @param <T> Base type of messages that can be sent/received by by this connector.
 * Note that {@link NetworkConnector} must be {@link NetworkConnectorConfig#setMessageCodec(CodecFactory) configured} to use a
 * {@link Codec} which is capable of serializing messages of this type.
 *
 * @see NetworkService
 * @see NetworkConnectorConfig
 */
public interface NetworkConnector<T> {
    /**
     * Returns the protocol identifier of this connector.
     *
     * @return Protocol identifier.
     *
     * @see NetworkConnectorConfig#setProtocol(String)
     */
    String protocol();

    /**
     * Creates a new {@link NetworkClient} instance.
     *
     * @return New client.
     */
    NetworkClient<T> newClient();
}
