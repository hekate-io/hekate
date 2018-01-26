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
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DefaultCodecService implements CodecService {
    private final CodecFactory<Object> codecFactory;

    public DefaultCodecService(CodecFactory<Object> codecFactory) {
        assert codecFactory != null : "Codec factory is null.";

        this.codecFactory = codecFactory;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> CodecFactory<T> codecFactory() {
        return (CodecFactory<T>)codecFactory;
    }

    @Override
    public void encodeToStream(Object obj, OutputStream out) throws IOException {
        ArgAssert.notNull(obj, "Object to encode");
        ArgAssert.notNull(out, "Output stream");

        doEncode(obj, out);
    }

    @Override
    public <T> T decodeFromByteArray(byte[] bytes) throws IOException {
        ArgAssert.notNull(bytes, "Byte array");

        ByteArrayInputStream buf = newInputBuffer(bytes);

        return doDecode(buf);
    }

    @Override
    public byte[] encodeToByteArray(Object obj) throws IOException {
        ArgAssert.notNull(obj, "Object to encode");

        ByteArrayOutputStream buf = newOutputBuffer();

        doEncode(obj, buf);

        return buf.toByteArray();
    }

    @Override
    public <T> T decodeFromStream(InputStream in) throws IOException {
        ArgAssert.notNull(in, "Input stream");

        return doDecode(in);
    }

    private void doEncode(Object obj, OutputStream out) throws IOException {
        codecFactory.createCodec().encode(obj, new StreamDataWriter(out));
    }

    @SuppressWarnings("unchecked")
    private <T> T doDecode(InputStream in) throws IOException {
        return (T)codecFactory.createCodec().decode(new StreamDataReader(in));
    }

    private ByteArrayOutputStream newOutputBuffer() {
        // TODO: Optimize.
        return new ByteArrayOutputStream();
    }

    private ByteArrayInputStream newInputBuffer(byte[] bytes) {
        // TODO: Optimize.
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public String toString() {
        return ToString.format(CodecService.class, this);
    }
}
