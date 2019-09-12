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

        public int hbInterval() {
            return hbInterval;
        }

        public int hbLossThreshold() {
            return hbLossThreshold;
        }

        public boolean isHbDisabled() {
            return hbDisabled;
        }

        @Override
        public Type type() {
            return Type.HANDSHAKE_ACCEPT;
        }
    }

    static class HandshakeReject extends NetworkProtocol {
        private final String reason;

        public HandshakeReject(String reason) {
            this.reason = reason;
        }

        public String reason() {
            return reason;
        }

        @Override
        public Type type() {
            return Type.HANDSHAKE_REJECT;
        }
    }

    static class HandshakeRequest extends NetworkProtocol {
        private final String protocol;

        private final int threadAffinity;

        private final Object payload;

        private final Codec<Object> codec;

        public HandshakeRequest(String protocol, Object payload, int threadAffinity) {
            this(protocol, payload, threadAffinity, null);
        }

        public HandshakeRequest(String protocol, Object payload, int threadAffinity, Codec<Object> codec) {
            this.protocol = protocol;
            this.payload = payload;
            this.codec = codec;
            this.threadAffinity = threadAffinity;
        }

        public String protocol() {
            return protocol;
        }

        public int threadAffinity() {
            return threadAffinity;
        }

        public Object payload() {
            return payload;
        }

        public Codec<Object> codec() {
            return codec;
        }

        @Override
        public Type type() {
            return Type.HANDSHAKE_REQUEST;
        }
    }

    static final class Heartbeat extends NetworkProtocol {
        public static final Heartbeat INSTANCE = new Heartbeat();

        private Heartbeat() {
            // No-op.
        }

        @Override
        public Type type() {
            return Type.HEARTBEAT;
        }
    }

    public abstract Type type();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
