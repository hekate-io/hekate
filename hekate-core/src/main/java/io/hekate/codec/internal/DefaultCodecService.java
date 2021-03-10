/*
 * Copyright 2021 The Hekate Project
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
import java.io.InputStream;
import java.io.OutputStream;

public class DefaultCodecService implements CodecService, EncoderDecoder<Object> {
    /** If buffer's capacity is larger than this value then it will not be reused and will be thrown away to be collected by the GC. */
    // TODO: Configurable maximum size of recyclable buffer.
    private static final int MAX_REUSABLE_BUFFER_SIZE = Integer.getInteger("io.hekate.codec.maxReusableBufferSize", 1024 * 1024);

    /** Codec factory. */
    private final CodecFactory<Object> factory;

    /** Pool of reusable buffers for encoding/decoding. */
    @ToStringIgnore
    private final ByteArrayOutputStreamPool buffers;

    /** Default codec for encoding/decoding objects of undefined type. */
    @ToStringIgnore
    private final EncoderDecoder<Object> codec;

    public DefaultCodecService(CodecFactory<Object> factory) {
        CodecFactory<Object> threadLocal = ThreadLocalCodecFactory.tryWrap(factory);

        this.factory = threadLocal;
        this.buffers = new ByteArrayOutputStreamPool(MAX_REUSABLE_BUFFER_SIZE);
        this.codec = new DefaultEncoderDecoder<>(buffers, threadLocal.createCodec());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CodecFactory<T> codecFactory() {
        return (CodecFactory<T>)factory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> EncoderDecoder<T> forType(Class<T> type) {
        return (EncoderDecoder<T>)this;
    }

    @Override
    public <T> EncoderDecoder<T> forType(Class<T> type, EncodeFunction<T> encode, DecodeFunction<T> decode) {
        ArgAssert.notNull(type, "Type");
        ArgAssert.notNull(encode, "Encode function");
        ArgAssert.notNull(decode, "Decode function");

        return new DefaultEncoderDecoder<>(type, buffers, encode, decode);
    }

    @Override
    public <T> EncoderDecoder<T> forFactory(CodecFactory<T> codecFactory) {
        ArgAssert.notNull(codecFactory, "Codec factory");

        return new DefaultEncoderDecoder<>(buffers, codecFactory.createCodec());
    }

    @Override
    public void encode(Object obj, OutputStream out) {
        codec.encode(obj, out);
    }

    @Override
    public void encode(Object obj, DataWriter out) {
        codec.encode(obj, out);
    }

    @Override
    public byte[] encode(Object obj) {
        return codec.encode(obj);
    }

    @Override
    public <T> byte[] encode(T obj, EncodeFunction<T> encoder) {
        ArgAssert.notNull(encoder, "Encode function");

        ByteArrayOutputStream buf = buffers.acquire();

        try {
            encoder.encodeUnchecked(obj, new StreamDataWriter(buf));

            return buf.toByteArray();
        } finally {
            buffers.recycle(buf);
        }
    }

    @Override
    public Object decode(InputStream in) {
        return codec.decode(in);
    }

    @Override
    public Object decode(DataReader in) {
        return codec.decode(in);
    }

    @Override
    public Object decode(byte[] bytes) {
        return codec.decode(bytes);
    }

    @Override
    public Object decode(byte[] bytes, int offset, int size) {
        return codec.decode(bytes, offset, size);
    }

    @Override
    public <T> T decode(byte[] bytes, DecodeFunction<T> decoder) {
        ArgAssert.notNull(bytes, "Byte array");
        ArgAssert.notNull(decoder, "Decode function");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes);

        return decoder.decodeUnchecked(new StreamDataReader(buf));
    }

    @Override
    public <T> T decode(byte[] bytes, int offset, int limit, DecodeFunction<T> decoder) {
        ArgAssert.notNull(bytes, "Byte array");
        ArgAssert.notNull(decoder, "Decode function");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes, offset, limit);

        return decoder.decodeUnchecked(new StreamDataReader(buf));
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
