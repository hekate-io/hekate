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

package io.hekate.codec.internal;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.EncoderDecoder;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DefaultCodecService implements CodecService {
    // TODO: Configurable size of recyclable buffers pool.
    private static final int MAX_POOL_SIZE = 1024;

    // TODO: Configurable maximum size of recyclable buffer.
    private static final int MAX_BUFFER_SIZE = 1024 * 1024;

    private final CodecFactory<Object> codecFactory;

    private final ByteArrayOutputStreamPool bufferPool;

    private final EncoderDecoder<Object> objEncodec;

    public DefaultCodecService(CodecFactory<Object> codecFactory) {
        assert codecFactory != null : "Codec factory is null.";

        this.codecFactory = codecFactory;

        bufferPool = new ByteArrayOutputStreamPool(MAX_POOL_SIZE, MAX_BUFFER_SIZE);

        objEncodec = new DefaultEncoderDecoder<>(bufferPool, codecFactory);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CodecFactory<T> codecFactory() {
        return (CodecFactory<T>)codecFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> EncoderDecoder<T> forType(Class<T> type) {
        return (EncoderDecoder<T>)this;
    }

    @Override
    public <T> EncoderDecoder<T> forFactory(CodecFactory<T> codecFactory) {
        ArgAssert.notNull(codecFactory, "Codec factory");

        return new DefaultEncoderDecoder<>(bufferPool, codecFactory);
    }

    @Override
    public void encodeToStream(Object obj, OutputStream out) throws IOException {
        objEncodec.encodeToStream(obj, out);
    }

    @Override
    public Object decodeFromByteArray(byte[] bytes) throws IOException {
        return objEncodec.decodeFromByteArray(bytes);
    }

    @Override
    public byte[] encodeToByteArray(Object obj) throws IOException {
        return objEncodec.encodeToByteArray(obj);
    }

    @Override
    public Object decodeFromStream(InputStream in) throws IOException {
        return objEncodec.decodeFromStream(in);
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
