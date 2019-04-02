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
import io.hekate.codec.DataReader;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * Buffered message.
 *
 * <p>
 * This class represents a buffered message that was received by a {@link NetworkEndpoint}. Obtaining real content of this message can be
 * performed by calling {@link #decode()} method that will apply an underlying
 * {@link NetworkConnectorConfig#setMessageCodec(CodecFactory) codec} to the buffered content of this message.
 * </p>
 *
 * <p>
 * Asynchronous handling of this message can be performed by calling {@link #handleAsync(Executor, Consumer) handleAsync(...)} method.
 * </p>
 *
 * @param <T> Message data type.
 */
public interface NetworkMessage<T> {
    /**
     * Preview function for {@link #preview(Preview)}.
     *
     * @param <V> Result type.
     */
    @FunctionalInterface
    interface Preview<V> {
        /**
         * Applies this function.
         *
         * @param rd Reader.
         *
         * @return Result of this function.
         *
         * @throws IOException Message reading failure.
         */
        V apply(DataReader rd) throws IOException;
    }

    /**
     * Preview function for {@link #previewInt(PreviewInt)}.
     */
    @FunctionalInterface
    interface PreviewInt {
        /**
         * Applies this function.
         *
         * @param rd Reader.
         *
         * @return Result of this function.
         *
         * @throws IOException Message reading failure.
         */
        int apply(DataReader rd) throws IOException;
    }

    /**
     * Preview function for {@link #previewLong(PreviewLong)}.
     */
    @FunctionalInterface
    interface PreviewLong {
        /**
         * Applies this function.
         *
         * @param rd Reader.
         *
         * @return Result of this function.
         *
         * @throws IOException Message reading failure.
         */
        long apply(DataReader rd) throws IOException;
    }

    /**
     * Preview function for {@link #previewDouble(PreviewDouble)}.
     */
    @FunctionalInterface
    interface PreviewDouble {
        /**
         * Applies this function.
         *
         * @param rd Reader.
         *
         * @return Result of this function.
         *
         * @throws IOException Message reading failure.
         */
        double apply(DataReader rd) throws IOException;
    }

    /**
     * Preview function for {@link #previewBoolean(PreviewBoolean)}.
     */
    @FunctionalInterface
    interface PreviewBoolean {
        /**
         * Applies this function.
         *
         * @param rd Reader.
         *
         * @return Result of this function.
         *
         * @throws IOException Message reading failure.
         */
        boolean apply(DataReader rd) throws IOException;
    }

    /**
     * Decodes this message by applying an underlying {@link Codec}.
     *
     * @return Decoded message.
     *
     * @throws IOException Message decoding failure.
     * @see NetworkConnectorConfig#setMessageCodec(CodecFactory)
     */
    T decode() throws IOException;

    /**
     * Asynchronously this message on a thread of the specified executor.
     *
     * @param worker Executor.
     * @param handler Handler.
     */
    void handleAsync(Executor worker, Consumer<NetworkMessage<T>> handler);

    /**
     * Applies the specified preview function to this message. Such function can be used to inspect the content of this message
     * without performing the complete {@link #decode() decoding}.
     *
     * @param preview Preview function.
     * @param <V> Type of result.
     *
     * @return Result of function application.
     *
     * @throws IOException Message reading failure.
     */
    <V> V preview(Preview<V> preview) throws IOException;

    /**
     * Applies the specified preview function to this message. Such function can be used to inspect the content of this message
     * without performing the complete {@link #decode() decoding}.
     *
     * @param preview Preview function.
     *
     * @return Result of function application.
     *
     * @throws IOException Message reading failure.
     */
    int previewInt(PreviewInt preview) throws IOException;

    /**
     * Applies the specified preview function to this message. Such function can be used to inspect the content of this message
     * without performing the complete {@link #decode() decoding}.
     *
     * @param preview Preview function.
     *
     * @return Result of function application.
     *
     * @throws IOException Message reading failure.
     */
    long previewLong(PreviewLong preview) throws IOException;

    /**
     * Applies the specified preview function to this message. Such function can be used to inspect the content of this message
     * without performing the complete {@link #decode() decoding}.
     *
     * @param preview Preview function.
     *
     * @return Result of function application.
     *
     * @throws IOException Message reading failure.
     */
    double previewDouble(PreviewDouble preview) throws IOException;

    /**
     * Applies the specified preview function to this message. Such function can be used to inspect the content of this message
     * without performing the complete {@link #decode() decoding}.
     *
     * @param preview Preview function.
     *
     * @return Result of function application.
     *
     * @throws IOException Message reading failure.
     */
    boolean previewBoolean(PreviewBoolean preview) throws IOException;
}
