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

package io.hekate.network.internal;

import io.netty.channel.Channel;
import java.util.Optional;

/**
 * Support interface for objects that has an internal Netty {@link Channel}.
 */
public interface NettyChannelSupport {
    /**
     * Returns an internal Netty channel.
     *
     * @return Netty channel.
     */
    Optional<Channel> nettyChannel();

    /**
     * Shortcut method to check if the specified object implements the {@link NettyChannelSupport} interface and returns its {@link
     * #nettyChannel() Netty channel}.
     *
     * @param obj Object to unwrap.
     *
     * @return Netty channel (if object is of {@link NettyChannelSupport} type and has a {@link #nettyChannel() channel}).
     */
    static Optional<Channel> unwrap(Object obj) {
        if (obj instanceof NettyChannelSupport) {
            return ((NettyChannelSupport)obj).nettyChannel();
        }

        return Optional.empty();
    }
}
