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

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.Service;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to data serialization API.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link CodecService} represents the {@link Service} interface adaptor for {@link CodecFactory} in order to make it easily
 * accessible via {@link Hekate#get(Class)} method. All data encoding/decoding operations are delegated to the {@link CodecFactory}
 * instance that is registered via {@link HekateBootstrap#setDefaultCodec(CodecFactory)} method.
 * </p>
 *
 * <h2>Accessing service</h2>
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
     * Encodes the specified object into the specified stream.
     *
     * @param obj Object.
     * @param out Stream.
     *
     * @throws IOException Signals encoding failure.
     * @see #decodeFromStream(InputStream)
     */
    void encodeToStream(Object obj, OutputStream out) throws IOException;

    /**
     * Decodes an object from the specified stream.
     *
     * @param in Stream to read data from.
     * @param <T> Object type.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     * @see #encodeToStream(Object, OutputStream)
     */
    <T> T decodeFromStream(InputStream in) throws IOException;

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
    byte[] encodeToByteArray(Object obj) throws IOException;

    /**
     * Decodes an object from the specified array of bytes.
     *
     * @param bytes Bytes.
     * @param <T> Object type.
     *
     * @return Decoded object.
     *
     * @throws IOException Signals decoding failure.
     */
    <T> T decodeFromByteArray(byte[] bytes) throws IOException;
}
