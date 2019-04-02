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

package io.hekate.network.netty;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecException;
import io.hekate.codec.DataReader;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.network.NetworkMessage;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.slf4j.Logger;

class NettyMessage extends InputStream implements DataReader, NetworkMessage<Object> {
    private final ByteBuf buf;

    private final Codec<Object> codec;

    private DataInputStream fallback;

    private Logger log;

    private Object decoded;

    public NettyMessage(ByteBuf buf, Codec<Object> codec) {
        this.codec = codec;
        this.buf = buf;
    }

    public void prepare(Logger log) {
        this.log = log;
    }

    public boolean release() {
        return buf.release();
    }

    @Override
    public Object decode() throws IOException {
        if (decoded == null) {
            try {
                decoded = codec.decode(this);

                if (decoded == null) {
                    throw new CodecException("Failed to decode message: codec returned null [codec=" + codec + ']');
                }

                // Mark buffer as fully consumed.
                skipRemainingBytes();

                if (log != null && log.isDebugEnabled()) {
                    log.debug("Message decoded [message={}]", decoded);
                }
            } catch (CodecException e) {
                // Mark buffer as fully consumed in case of an error
                // in order to suppress errors reporting on dirty buffer.
                skipRemainingBytes();

                throw e;
            } catch (IOException | RuntimeException | Error e) {
                // Mark buffer as fully consumed in case of an error
                // in order to suppress errors reporting on dirty buffer.
                skipRemainingBytes();

                throw new CodecException("Failed to decode message.", e);
            }
        }

        return decoded;
    }

    @Override
    public void handleAsync(Executor worker, Consumer<NetworkMessage<Object>> handler) {
        ArgAssert.notNull(worker, "Worker");
        ArgAssert.notNull(handler, "Handler");

        buf.retain();

        worker.execute(() -> {
            try {
                handler.accept(this);
            } finally {
                buf.release();
            }
        });
    }

    @Override
    public <V> V preview(Preview<V> preview) throws IOException {
        int idx = buf.readerIndex();

        buf.readerIndex(0);

        try {
            return preview.apply(this);
        } finally {
            buf.readerIndex(idx);
        }
    }

    @Override
    public int previewInt(PreviewInt preview) throws IOException {
        int idx = buf.readerIndex();

        buf.readerIndex(0);

        try {
            return preview.apply(this);
        } finally {
            buf.readerIndex(idx);
        }
    }

    @Override
    public long previewLong(PreviewLong preview) throws IOException {
        int idx = buf.readerIndex();

        buf.readerIndex(0);

        try {
            return preview.apply(this);
        } finally {
            buf.readerIndex(idx);
        }
    }

    @Override
    public double previewDouble(PreviewDouble preview) throws IOException {
        int idx = buf.readerIndex();

        buf.readerIndex(0);

        try {
            return preview.apply(this);
        } finally {
            buf.readerIndex(idx);
        }
    }

    @Override
    public boolean previewBoolean(PreviewBoolean preview) throws IOException {
        int idx = buf.readerIndex();

        buf.readerIndex(0);

        try {
            return preview.apply(this);
        } finally {
            buf.readerIndex(idx);
        }
    }

    @Override
    public InputStream asStream() {
        return this;
    }

    @Override
    public int available() throws IOException {
        return buf.readableBytes();
    }

    @Override
    public int read() throws IOException {
        if (!buf.isReadable()) {
            return -1;
        }

        return buf.readByte() & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int available = available();

        if (available == 0) {
            return -1;
        }

        len = Math.min(available, len);

        buf.readBytes(b, off, len);

        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n > Integer.MAX_VALUE) {
            return skipBytes(Integer.MAX_VALUE);
        } else {
            return skipBytes((int)n);
        }
    }

    @Override
    public boolean readBoolean() throws IOException {
        try {
            return buf.readBoolean();
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    @Override
    public byte readByte() throws IOException {
        try {
            return buf.readByte();
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    @Override
    public char readChar() throws IOException {
        return (char)readShort();
    }

    @Override
    public double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    @Override
    public float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        try {
            buf.readBytes(b, off, len);
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    @Override
    public int readInt() throws IOException {
        try {
            return buf.readInt();
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    @Override
    @Deprecated
    public String readLine() throws IOException {
        if (fallback == null) {
            fallback = new DataInputStream(this);
        }

        return fallback.readLine();
    }

    @Override
    public long readLong() throws IOException {
        try {
            return buf.readLong();
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    @Override
    public short readShort() throws IOException {
        try {
            return buf.readShort();
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    @Override
    public String readUTF() throws IOException {
        return utf(buf);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return readByte() & 0xff;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return readShort() & 0xffff;
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int minBytes = Math.min(available(), n);

        buf.skipBytes(minBytes);

        return minBytes;
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public void reset() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mark(int readLimit) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public <T> NetworkMessage<T> cast() {
        return (NetworkMessage<T>)this;
    }

    static String utf(ByteBuf buf) throws IOException {
        try {
            int len = buf.readInt();

            String string = buf.toString(buf.readerIndex(), len, StandardCharsets.UTF_8);

            buf.skipBytes(len);

            return string;
        } catch (IndexOutOfBoundsException e) {
            throw endOfStream(e);
        }
    }

    private void skipRemainingBytes() {
        int remaining = buf.readableBytes();

        if (remaining > 0) {
            buf.skipBytes(remaining);
        }
    }

    private static EOFException endOfStream(IndexOutOfBoundsException e) {
        return new EOFException(e.getMessage());
    }

    @Override
    public String toString() {
        return NetworkMessage.class.getSimpleName() + "[size=" + buf.capacity() + ", decoded=" + decoded + ']';
    }
}
