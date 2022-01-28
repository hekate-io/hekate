/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecService;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.codec.DecodeFunction;
import io.hekate.codec.EncodeFunction;
import io.hekate.codec.EncoderDecoder;
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

class DefaultEncoderDecoder<T> implements EncoderDecoder<T> {
    private final Class<T> type;

    private final ByteArrayOutputStreamPool buffers;

    private final EncodeFunction<T> encoder;

    private final DecodeFunction<T> decoder;

    public DefaultEncoderDecoder(ByteArrayOutputStreamPool buffers, Codec<T> codec) {
        this.type = codec.baseType();
        this.buffers = buffers;
        this.encoder = codec;
        this.decoder = codec;
    }

    public DefaultEncoderDecoder(
        Class<T> type,
        ByteArrayOutputStreamPool buffers,
        EncodeFunction<T> encoder,
        DecodeFunction<T> decoder
    ) {
        this.type = type;
        this.buffers = buffers;
        this.encoder = encoder;
        this.decoder = decoder;
    }

    @Override
    public void encode(T obj, OutputStream out) {
        checkType(obj);

        encode(obj, (DataWriter)new StreamDataWriter(out));
    }

    @Override
    public void encode(T obj, DataWriter out) {
        checkType(obj);

        ArgAssert.notNull(obj, "Object to encode");
        ArgAssert.notNull(out, "Output stream");

        encoder.encodeUnchecked(obj, out);
    }

    @Override
    public byte[] encode(T obj) {
        ArgAssert.notNull(obj, "Object to encode");

        checkType(obj);

        ByteArrayOutputStream buf = buffers.acquire();

        try {
            encoder.encodeUnchecked(obj, new StreamDataWriter(buf));

            return buf.toByteArray();
        } finally {
            buffers.recycle(buf);
        }
    }

    @Override
    public T decode(byte[] bytes) {
        ArgAssert.notNull(bytes, "Byte array");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes);

        return decoder.decodeUnchecked(new StreamDataReader(buf));
    }

    @Override
    public T decode(byte[] bytes, int offset, int limit) {
        ArgAssert.notNull(bytes, "Byte array");

        ByteArrayInputStream buf = new ByteArrayInputStream(bytes, offset, limit);

        return decoder.decodeUnchecked(new StreamDataReader(buf));
    }

    @Override
    public T decode(InputStream in) {
        return decode((DataReader)new StreamDataReader(in));
    }

    @Override
    public T decode(DataReader in) {
        ArgAssert.notNull(in, "Input stream");

        return decoder.decodeUnchecked(in);
    }

    private void checkType(T obj) {
        if (obj != null && !type.isInstance(obj)) {
            throw new ClassCastException("Can't encode/decode " + obj.getClass().getName() + " (expected " + type.getName() + ")");
        }
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
