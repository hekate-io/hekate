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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Interface for components that can perform encoding/decoding operations.
 *
 * @param <T> Base type of objects that can be encoded/decoded.
 *
 * @see CodecService#forType(Class)
 * @see CodecService#forFactory(CodecFactory)
 */
public interface EncoderDecoder<T> {
    /**
     * Encodes the specified object into the specified stream.
     *
     * @param obj Object.
     * @param out Stream.
     *
     * @throws IOException Signals encoding failure.
     * @see #decode(InputStream)
     */
    void encode(T obj, OutputStream out) throws IOException;

    /**
     * Encodes the specified object into the specified writer.
     *
     * @param obj Object.
     * @param out Writer.
     *
     * @throws IOException Signals encoding failure.
     * @see #decode(DataReader)
     */
    void encode(T obj, DataWriter out) throws IOException;

    /**
     * Encodes the specified object into an array of bytes.
     *
     * @param obj Object.
     *
     * @return Bytes.
     *
     * @throws IOException Signals encoding failure.
     * @see #decode(byte[])
     */
    byte[] encode(T obj) throws IOException;

    /**
     * Decodes an object from the specified stream.
     *
     * @param in Stream to read data from.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     * @see #encode(Object, OutputStream)
     */
    T decode(InputStream in) throws IOException;

    /**
     * Decodes an object from the specified reader.
     *
     * @param in Reader.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     * @see #encode(Object, DataWriter)
     */
    T decode(DataReader in) throws IOException;

    /**
     * Decodes an object from the specified array of bytes.
     *
     * @param bytes Bytes.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     */
    T decode(byte[] bytes) throws IOException;

    /**
     * Decodes an object from the specified array of bytes.
     *
     * @param bytes Bytes.
     * @param offset Offset of the first byte to read.
     * @param limit Maximum number of bytes to read.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     */
    T decode(byte[] bytes, int offset, int limit) throws IOException;
}
