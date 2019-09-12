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

import io.hekate.network.NetworkServerFailure;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;

class NettyServerFailure implements NetworkServerFailure {
    private static class NettyResolution implements Resolution {
        private final boolean failure;

        private long retryDelay;

        private InetSocketAddress retryAddress;

        public NettyResolution(boolean failure) {
            this.failure = failure;
        }

        @Override
        public boolean isFailure() {
            return failure;
        }

        @Override
        public long retryDelay() {
            return retryDelay;
        }

        @Override
        public InetSocketAddress retryAddress() {
            return retryAddress;
        }

        @Override
        public Resolution withRetryDelay(long retryDelay) {
            this.retryDelay = retryDelay;

            return this;
        }

        @Override
        public Resolution withRetryAddress(InetSocketAddress retryAddress) {
            this.retryAddress = retryAddress;

            return this;
        }
    }

    private final Throwable cause;

    private final int attempt;

    private final InetSocketAddress lastTriedAddress;

    public NettyServerFailure(Throwable cause, int attempt, InetSocketAddress lastTriedAddress) {
        this.attempt = attempt;
        this.lastTriedAddress = lastTriedAddress;
        this.cause = cause;
    }

    @Override
    public Throwable cause() {
        return cause;
    }

    @Override
    public int attempt() {
        return attempt;
    }

    @Override
    public InetSocketAddress lastTriedAddress() {
        return lastTriedAddress;
    }

    @Override
    public Resolution fail() {
        return new NettyResolution(true);
    }

    @Override
    public Resolution retry() {
        return new NettyResolution(false);
    }

    @Override
    public String toString() {
        return ToString.format(NetworkServerFailure.class, this);
    }
}
