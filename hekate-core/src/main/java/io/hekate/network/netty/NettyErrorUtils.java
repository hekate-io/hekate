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

package io.hekate.network.netty;

import io.hekate.network.NetworkConnectException;
import io.hekate.network.NetworkConnectTimeoutException;
import io.hekate.network.NetworkEndpointClosedException;
import io.hekate.network.NetworkException;
import io.hekate.network.NetworkTimeoutException;
import io.netty.channel.ConnectTimeoutException;
import io.netty.handler.codec.DecoderException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedChannelException;
import javax.net.ssl.SSLException;

final class NettyErrorUtils {
    private NettyErrorUtils() {
        // No-op.
    }

    public static Throwable unwrap(Throwable err) {
        if (err == null) {
            return null;
        } else if (err instanceof DecoderException && err.getCause() instanceof SSLException) {
            return doUnwrap(err.getCause());
        } else {
            return doUnwrap(err);
        }
    }

    private static Throwable doUnwrap(Throwable err) {
        if (err instanceof ConnectTimeoutException) {
            return new NetworkConnectTimeoutException(err.getMessage(), err);
        } else if (err instanceof SocketTimeoutException) {
            return new NetworkTimeoutException(err.getMessage(), err);
        } else if (err instanceof ConnectException) {
            return new NetworkConnectException(err.getMessage(), err);
        } else if (err instanceof ClosedChannelException) {
            return new NetworkEndpointClosedException(err.getMessage(), err);
        } else if (err instanceof SocketException) {
            return new NetworkException(err.getMessage(), err);
        } else if (err instanceof IOException) {
            return new NetworkException(err.getMessage(), err);
        }

        return err;
    }
}
