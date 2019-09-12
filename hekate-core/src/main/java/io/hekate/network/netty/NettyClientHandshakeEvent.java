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

package io.hekate.network.netty;

import io.hekate.network.netty.NetworkProtocol.HandshakeAccept;

class NettyClientHandshakeEvent {
    private final int hbInterval;

    private final int hbLossThreshold;

    private final boolean hbDisabled;

    public NettyClientHandshakeEvent(HandshakeAccept accept) {
        this(accept.hbInterval(), accept.hbLossThreshold(), accept.isHbDisabled());
    }

    public NettyClientHandshakeEvent(int hbInterval, int hbLossThreshold, boolean hbDisabled) {
        this.hbInterval = hbInterval;
        this.hbLossThreshold = hbLossThreshold;
        this.hbDisabled = hbDisabled;
    }

    public int hbInterval() {
        return hbInterval;
    }

    public int hbLossThreshold() {
        return hbLossThreshold;
    }

    public boolean isHbDisabled() {
        return hbDisabled;
    }
}
