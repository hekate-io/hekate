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

package io.hekate.codec;

/**
 * Data serialization codec.
 *
 * <p>
 * This interface is responsible for encoding/decoding data to/from its binary form for transferring over the network or storing on disk.
 * Typically instances of this interface are not constructed directly, but {@link CodecFactory#createCodec()} should be used instead.
 * </p>
 *
 * <p>
 * This interface is assumed to be not thread safe and new instance should be created via {@link CodecFactory} for each thread that
 * performs data encoding/decoding.
 * </p>
 *
 * @param <T> Base data type that is supported by this codec.
 */
public interface Codec<T> extends EncodeFunction<T>, DecodeFunction<T> {
    /**
     * Returns {@code true} if this codec maintains an internal state and can't be shared among multiple threads or network connections.
     *
     * <p>
     * Stateful codecs are optimized for encoding/decoding data between two stream-based endpoints (i.e. endpoints that maintain some kind
     * of persistent connection, for example TCP socket connections). Stateful codecs can rely on the fact that encoded data is always
     * decoded by the same codec on the receiver side and can perform various optimizations like data compression based on auto-populating
     * dictionaries.
     * </p>
     *
     * @return {@code true} if this codec maintains an internal state and can't be shared among multiple threads or network connections.
     */
    boolean isStateful();

    /**
     * Returns the base data type that is supported by this codec.
     *
     * @return Base data type that is supported by this codec.
     */
    Class<T> baseType();
}
