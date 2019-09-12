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

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.Service;
import java.io.IOException;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to data serialization API.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link CodecService} represents the {@link Service} interface adaptor for {@link CodecFactory} in order to make it easily
 * accessible via {@link Hekate#codec()} method. All data encoding/decoding operations are delegated to the {@link CodecFactory}
 * instance that is registered via the {@link HekateBootstrap#setDefaultCodec(CodecFactory)} method.
 * </p>
 *
 * <h2>Accessing the Service</h2>
 * <p>
 * Instances of this service can be obtained via {@link Hekate#codec()} method as shown in the example below:
 * ${source: codec/CodecServiceJavadocTest.java#access}
 * </p>
 *
 * @see Codec
 * @see HekateBootstrap#setDefaultCodec(CodecFactory)
 */
public interface CodecService extends Service {
    /**
     * Returns an underlying codec factory (see {@link HekateBootstrap#setDefaultCodec(CodecFactory)}).
     *
     * @param <T> Type that should be supported by the returned codec factory.
     *
     * @return Codec factory.
     */
    <T> CodecFactory<T> codecFactory();

    /**
     * Returns encoder/decoder for the specified type that uses the underlying {@link #codecFactory()} of this service for encoding/decoding
     * operations.
     *
     * @param type Data type.
     * @param <T> Data type.
     *
     * @return Encoder/decoder for the specified type.
     */
    <T> EncoderDecoder<T> forType(Class<T> type);

    /**
     * Returns encoder/decoder for the codec factory.
     *
     * @param codecFactory Codec factory.
     * @param <T> Data type.
     *
     * @return Encoder/decoder for the specified factory.
     */
    <T> EncoderDecoder<T> forFactory(CodecFactory<T> codecFactory);

    /**
     * Encodes the specified object via the specified function and returns a byte array of encoded data.
     *
     * @param obj Object to encode (can be {@code null}, in such case the {@code encoder} function's parameter will be {@code null} too).
     * @param encoder Encoder function.
     * @param <T> Object type.
     *
     * @return Byte array of encoded data.
     *
     * @throws IOException If object couldn't be decoded.
     */
    <T> byte[] encode(T obj, EncodeFunction<T> encoder) throws IOException;

    /**
     * Decodes an object from the specified byte array by using the supplied decode function.
     *
     * @param bytes Bytes.
     * @param decoder Decode function.
     * @param <T> Decoded object type.
     *
     * @return Decoded object.
     *
     * @throws IOException If object couldn't be decoded.
     */
    <T> T decode(byte[] bytes, DecodeFunction<T> decoder) throws IOException;

    /**
     * Decodes an object from the specified byte array by using the supplied decode function.
     *
     * @param bytes Bytes.
     * @param offset Offset to
     * @param limit Maximum number of bytes to read.
     * @param decoder Decode function.
     * @param <T> Decoded object type.
     *
     * @return Decoded object.
     *
     * @throws IOException If object couldn't be decoded.
     */
    <T> T decode(byte[] bytes, int offset, int limit, DecodeFunction<T> decoder) throws IOException;
}
