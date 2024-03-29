/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.messaging.intercept;

import io.hekate.messaging.MessagingChannelConfig;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Message interceptor.
 *
 * <p>
 * This is a marker interface for {@link ClientMessageInterceptor} and {@link ServerMessageInterceptor}.
 * </p>
 *
 * <p>
 * Instances of this interface can be {@link MessagingChannelConfig#setInterceptors(List) registered} to a messaging channel in order to
 * intercept and transform messages on the client side (via {@link ClientMessageInterceptor}), on the server side
 * (via {@link ServerMessageInterceptor}) or on both sides (via {@link AllMessageInterceptor}).
 * </p>
 *
 * <pre>{@code
 *              Client Node                                             Server Node
 *       (ClientMessageInterceptor)                               (ServerMessageInterceptor)
 * +-------------------------------------+       request       +-----------------------------+
 * | interceptClientSend(...)            + ------------------> + interceptServerReceive(...) |
 * +-------------------------------------+                     +-------------+---------------+
 *                                                                    process request
 * +-------------------------------------+       response      +-------------v---------------+
 * | interceptClientReceiveResponse(...) + <------------------ + interceptServerSend(...)    |
 * +-------------------------------------+                     +-----------------------------+
 * }</pre>
 *
 * @see MessagingChannelConfig#setInterceptors(List)
 */
public interface MessageInterceptor {
    // No-op.
}
