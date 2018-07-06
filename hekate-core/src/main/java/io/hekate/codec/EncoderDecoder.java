/*
 * Copyright 2018 The Hekate Project
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
 * Interface for components that can perform multi-purpose encoding/decoding operations.
 *
 * @param <T> Base type of objects that can be encoded/decoded.
 */
public interface EncoderDecoder<T> {
    /**
     * Encodes the specified object into the specified stream.
     *
     * @param obj Object.
     * @param out Stream.
     *
     * @throws IOException Signals encoding failure.
     * @see #decodeFromStream(InputStream)
     */
    void encodeToStream(T obj, OutputStream out) throws IOException;

    /**
     * Decodes an object from the specified stream.
     *
     * @param in Stream to read data from.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     * @see #encodeToStream(Object, OutputStream)
     */
    T decodeFromStream(InputStream in) throws IOException;

    /**
     * Encodes the specified object into an array of bytes.
     *
     * @param obj Object.
     *
     * @return Bytes.
     *
     * @throws IOException Signals encoding failure.
     * @see #decodeFromByteArray(byte[])
     */
    byte[] encodeToByteArray(T obj) throws IOException;

    /**
     * Decodes an object from the specified array of bytes.
     *
     * @param bytes Bytes.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     */
    T decodeFromByteArray(byte[] bytes) throws IOException;

    /**
     * Decodes an object from the specified array of bytes.
     *
     * @param bytes Bytes.
     * @param offset Offset of the first byte to read.
     * @param size Maximum number of bytes to read.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     */
    T decodeFromByteArray(byte[] bytes, int offset, int size) throws IOException;
}
