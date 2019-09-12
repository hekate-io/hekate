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

package io.hekate.codec.internal;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.codec.DecodeFunction;
import io.hekate.codec.EncodeFunction;
import io.hekate.codec.EncoderDecoder;
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.codec.ThreadLocalCodecFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DefaultCodecService implements CodecService, EncoderDecoder<Object> {
    // TODO: Configurable maximum size of recyclable buffer.
    private static final int MAX_REUSABLE_BUFFER_SIZE = Integer.getInteger("io.hekate.codec.maxReusableBufferSize", 1024 * 1024);

    private final CodecFactory<Object> codecFactory;

    @ToStringIgnore
    private final ByteArrayOutputStreamPool bufferPool;

    @ToStringIgnore
    private final EncoderDecoder<Object> objCodec;

    public DefaultCodecService(CodecFactory<Object> codecFactory) {
        assert codecFactory != null : "Codec factory is null.";

        CodecFactory<Object> threadLocalCodecFactory = ThreadLocalCodecFactory.tryWrap(codecFactory);

        this.codecFactory = threadLocalCodecFactory;
        this.bufferPool = new ByteArrayOutputStreamPool(MAX_REUSABLE_BUFFER_SIZE);
        this.objCodec = new DefaultEncoderDecoder<>(bufferPool, threadLocalCodecFactory);
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
    public void encode(Object obj, OutputStream out) throws IOException {
        objCodec.encode(obj, out);
    }

    @Override
    public void encode(Object obj, DataWriter out) throws IOException {
        objCodec.encode(obj, out);
    }

    @Override
    public byte[] encode(Object obj) throws IOException {
        return objCodec.encode(obj);
    }

    @Override
    public <T> byte[] encode(T obj, EncodeFunction<T> encoder) throws IOException {
        ArgAssert.notNull(encoder, "Encode function");

        ByteArrayOutputStream buf = bufferPool.acquire();

        try {
            encoder.encode(obj, new StreamDataWriter(buf));

            return buf.toByteArray();
        } finally {
            bufferPool.recycle(buf);
        }
    }

    @Override
    public Object decode(InputStream in) throws IOException {
        return objCodec.decode(in);
    }

    @Override
    public Object decode(DataReader in) throws IOException {
        return objCodec.decode(in);
    }

    @Override
    public Object decode(byte[] bytes) throws IOException {
        return objCodec.decode(bytes);
    }

    @Override
    public Object decode(byte[] bytes, int offset, int size) throws IOException {
        return objCodec.decode(bytes, offset, size);
    }

    @Override
    public <T> T decode(byte[] bytes, DecodeFunction<T> decoder) throws IOException {
        ArgAssert.notNull(bytes, "Byte array");
        ArgAssert.notNull(decoder, "Decode function");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes);

        return decoder.decode(new StreamDataReader(buf));
    }

    @Override
    public <T> T decode(byte[] bytes, int offset, int limit, DecodeFunction<T> decoder) throws IOException {
        ArgAssert.notNull(bytes, "Byte array");
        ArgAssert.notNull(decoder, "Decode function");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes, offset, limit);

        return decoder.decode(new StreamDataReader(buf));
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
