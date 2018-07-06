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
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

class DefaultEncoderDecoder<T> implements EncoderDecoder<T> {
    private final ByteArrayOutputStreamPool bufferPool;

    private final CodecFactory<T> codecFactory;

    public DefaultEncoderDecoder(ByteArrayOutputStreamPool bufferPool, CodecFactory<T> codecFactory) {
        assert bufferPool != null : "Buffer pool is null.";
        assert codecFactory != null : "Codec factory is null.";

        this.codecFactory = codecFactory;
        this.bufferPool = bufferPool;
    }

    @Override
    public void encodeToStream(T obj, OutputStream out) throws IOException {
        ArgAssert.notNull(obj, "Object to encode");
        ArgAssert.notNull(out, "Output stream");

        codecFactory.createCodec().encode(obj, new StreamDataWriter(out));
    }

    @Override
    public T decodeFromByteArray(byte[] bytes) throws IOException {
        ArgAssert.notNull(bytes, "Byte array");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes);

        return codecFactory.createCodec().decode(new StreamDataReader(buf));
    }

    @Override
    public T decodeFromByteArray(byte[] bytes, int offset, int size) throws IOException {
        ArgAssert.notNull(bytes, "Byte array");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes, offset, size);

        return codecFactory.createCodec().decode(new StreamDataReader(buf));
    }

    @Override
    public byte[] encodeToByteArray(T obj) throws IOException {
        ArgAssert.notNull(obj, "Object to encode");

        ByteArrayOutputStream buf = bufferPool.acquire();

        try {
            codecFactory.createCodec().encode(obj, new StreamDataWriter(buf));

            return buf.toByteArray();
        } finally {
            bufferPool.recycle(buf);
        }
    }

    @Override
    public T decodeFromStream(InputStream in) throws IOException {
        ArgAssert.notNull(in, "Input stream");

        return codecFactory.createCodec().decode(new StreamDataReader(in));
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
