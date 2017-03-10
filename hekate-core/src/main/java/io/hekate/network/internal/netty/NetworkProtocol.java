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

package io.hekate.network.internal.netty;

import io.hekate.codec.Codec;
import io.hekate.util.format.ToString;

abstract class NetworkProtocol {
    enum Type {
        HANDSHAKE_REQUEST,

        HANDSHAKE_ACCEPT,

        HANDSHAKE_REJECT,

        HEARTBEAT,
    }

    static class HandshakeAccept extends NetworkProtocol {
        private final int hbInterval;

        private final int hbLossThreshold;

        private final boolean hbDisabled;

        public HandshakeAccept(int hbInterval, int hbLossThreshold, boolean hbDisabled) {
            this.hbInterval = hbInterval;
            this.hbLossThreshold = hbLossThreshold;
            this.hbDisabled = hbDisabled;
        }

        public int getHbInterval() {
            return hbInterval;
        }

        public int getHbLossThreshold() {
            return hbLossThreshold;
        }

        public boolean isHbDisabled() {
            return hbDisabled;
        }

        @Override
        public Type getType() {
            return Type.HANDSHAKE_ACCEPT;
        }
    }

    static class HandshakeReject extends NetworkProtocol {
        private final String reason;

        public HandshakeReject(String reason) {
            this.reason = reason;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public Type getType() {
            return Type.HANDSHAKE_REJECT;
        }
    }

    static class HandshakeRequest extends NetworkProtocol {
        private final String protocol;

        private final Object payload;

        private final Codec<Object> codec;

        public HandshakeRequest(String protocol, Object payload) {
            this(protocol, payload, null);
        }

        public HandshakeRequest(String protocol, Object payload, Codec<Object> codec) {
            this.protocol = protocol;
            this.payload = payload;
            this.codec = codec;
        }

        public String getProtocol() {
            return protocol;
        }

        public Object getPayload() {
            return payload;
        }

        public Codec<Object> getCodec() {
            return codec;
        }

        @Override
        public Type getType() {
            return Type.HANDSHAKE_REQUEST;
        }
    }

    static final class Heartbeat extends NetworkProtocol {
        public static final Heartbeat INSTANCE = new Heartbeat();

        private Heartbeat() {
            // No-op.
        }

        @Override
        public Type getType() {
            return Type.HEARTBEAT;
        }
    }

    public abstract Type getType();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
