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

import io.hekate.HekateTestBase;
import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class NettyMessageReadWritTest extends HekateTestBase {
    interface WriterTask<T> {
        void write(T obj, DataWriter out) throws IOException;
    }

    interface ReaderTask {
        Object read(DataReader in) throws IOException;
    }

    @SuppressWarnings("unchecked")
    private final Codec<Object> fakeCodec = (Codec<Object>)mock(Codec.class);

    private final ByteBufDataWriter writer = new ByteBufDataWriter();

    @Test
    public void testAvailable() throws IOException {
        ByteBuf buf = write((v, out) -> out.writeInt(v), 123);

        NettyMessage reader = new NettyMessage(buf, fakeCodec);

        assertEquals(4, reader.available());
    }

    @Test
    public void testMarkSupported() throws IOException {
        NettyMessage reader = new NettyMessage(Unpooled.EMPTY_BUFFER, fakeCodec);

        assertFalse(reader.markSupported());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testMark() throws IOException {
        NettyMessage reader = new NettyMessage(Unpooled.EMPTY_BUFFER, fakeCodec);

        reader.mark(1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReset() throws IOException {
        NettyMessage reader = new NettyMessage(Unpooled.EMPTY_BUFFER, fakeCodec);

        reader.reset();
    }

    @Test
    public void testSkip() throws IOException {
        ByteBuf buf = write((v, out) -> out.writeInt(v), 111, 222, 333, 444);

        NettyMessage reader = new NettyMessage(buf, fakeCodec);

        assertEquals(4 * 4, reader.available());

        assertEquals(4, reader.skip(4));

        assertEquals(222, reader.readInt());

        assertEquals(4, reader.skip(4));

        assertEquals(444, reader.readInt());
    }

    @Test(expected = EOFException.class)
    public void testEofException() throws IOException {
        ByteBuf buf = write((v, out) -> out.writeInt(v), 111);

        NettyMessage reader = new NettyMessage(buf, fakeCodec);

        assertEquals(111, reader.readInt());

        reader.readByte();
    }

    @Test(expected = EOFException.class)
    public void testReadFullyEofException() throws IOException {
        ByteBuf buf = write((v, out) -> out.writeInt(v), 111);

        NettyMessage reader = new NettyMessage(buf, fakeCodec);

        assertEquals(111, reader.readInt());

        reader.readFully(new byte[100]);
    }

    @Test
    public void testReadEmpty() throws IOException {
        ByteBuf buf = write((v, out) -> out.writeInt(v), 111);

        NettyMessage reader = new NettyMessage(buf, fakeCodec);

        assertEquals(111, reader.readInt());

        assertEquals(-1, reader.read());
        assertEquals(-1, reader.read(new byte[100]));
    }

    @Test
    public void testBytes() throws Exception {
        byte[] bytes = new byte[10];

        Arrays.fill(bytes, (byte)100);

        check(input(bytes), (v, out) -> out.write(v), in -> {
            byte[] read = new byte[bytes.length];

            assertTrue(in.asStream().read(read) > 0);

            return read;
        });
    }

    @Test
    public void testBytesReadFully() throws Exception {
        byte[] bytes = new byte[10];

        Arrays.fill(bytes, (byte)100);

        check(input(bytes), (v, out) -> out.write(v), in -> {
            byte[] read = new byte[bytes.length];

            in.readFully(read);

            return read;
        });
    }

    @Test
    public void testBoolean() throws Exception {
        check(input(true, false), (v, out) -> out.writeBoolean(v), DataInput::readBoolean);
    }

    @Test
    public void testByte() throws Exception {
        check(input((byte)0, Byte.MIN_VALUE, Byte.MAX_VALUE), (v, out) -> out.writeByte(v), DataInput::readByte);
        check(input((byte)0, Byte.MIN_VALUE, Byte.MAX_VALUE), (v, out) -> out.write(v), in -> (byte)in.asStream().read());
    }

    @Test
    public void testBytesString() throws Exception {
        String str = "abc";

        check(input(str), (v, out) -> out.writeBytes(v), in -> {
            StringBuilder s = new StringBuilder();

            for (int i = 0; i < str.length(); i++) {
                s.append((char)in.asStream().read());
            }

            return s.toString();
        });
    }

    @Test
    public void testChars() throws Exception {
        String str = "abc";

        check(input(str), (v, out) -> out.writeChars(v), in -> {
            StringBuilder s = new StringBuilder();

            for (int i = 0; i < str.length(); i++) {
                s.append(in.readChar());
            }

            return s.toString();
        });
    }

    @Test
    public void testReadLines() throws Exception {
        String str = "abc_def_ghi jkl mno pqr";

        check(input(str), (obj, out) -> out.writeBytes(str.replaceAll("_", "\n").replaceAll(" ", "\r\n")), in -> {
            String s = "";

            s += in.readLine() + "_";
            s += in.readLine() + "_";
            s += in.readLine() + " ";
            s += in.readLine() + " ";
            s += in.readLine() + " ";
            s += in.readLine();

            return s;
        });
    }

    @Test
    public void testShort() throws Exception {
        check(input((short)0, Short.MIN_VALUE, Short.MAX_VALUE), (v, out) -> out.writeShort(v), DataInput::readShort);
    }

    @Test
    public void testInt() throws Exception {
        check(input(0, Integer.MIN_VALUE, Integer.MAX_VALUE), (v, out) -> out.writeInt(v), DataInput::readInt);
    }

    @Test
    public void testVarInt() throws Exception {
        check(input(0, Integer.MIN_VALUE, Integer.MAX_VALUE), (v, out) -> out.writeVarInt(v), DataReader::readVarInt);
    }

    @Test
    public void testUnsignedVarInt() throws Exception {
        check(input(0, 99, Integer.MAX_VALUE), (v, out) -> out.writeVarIntUnsigned(v), DataReader::readVarIntUnsigned);
    }

    @Test
    public void testLong() throws Exception {
        check(input(0L, Long.MIN_VALUE, Long.MAX_VALUE), (v, out) -> out.writeLong(v), DataInput::readLong);
    }

    @Test
    public void testVarLong() throws Exception {
        check(input(0L, Long.MIN_VALUE, Long.MAX_VALUE), (v, out) -> out.writeVarLong(v), DataReader::readVarLong);
    }

    @Test
    public void testUnsignedVarLong() throws Exception {
        check(input(0L, 99L, Long.MAX_VALUE), (v, out) -> out.writeVarLongUnsigned(v), DataReader::readVarLongUnsigned);
    }

    @Test
    public void testFloat() throws Exception {
        check(input(0f, 0.1f, Float.MIN_VALUE, Float.MAX_VALUE), (v, out) -> out.writeFloat(v), DataInput::readFloat);
    }

    @Test
    public void testDouble() throws Exception {
        check(input(0d, 0.1d, Double.MIN_VALUE, Double.MAX_VALUE), (v, out) -> out.writeDouble(v), DataInput::readDouble);
    }

    @Test
    public void testBigDecimal() throws Exception {
        BigDecimal v1 = BigDecimal.valueOf(Double.MIN_VALUE);
        BigDecimal v2 = BigDecimal.valueOf(Double.MAX_VALUE).multiply(BigDecimal.valueOf(2));
        BigDecimal v3 = BigDecimal.ZERO;
        BigDecimal v4 = BigDecimal.ONE;
        BigDecimal v5 = BigDecimal.TEN;
        BigDecimal v6 = new BigDecimal(100);
        BigDecimal v7 = new BigDecimal(BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2)));

        check(input(v1, v2, v3, v4, v5, v6, v7), (v, out) -> out.writeBigDecimal(v), DataReader::readBigDecimal);
    }

    @Test
    public void testBigInteger() throws Exception {
        BigInteger v1 = BigInteger.valueOf(Long.MIN_VALUE);
        BigInteger v2 = BigInteger.valueOf(Long.MAX_VALUE).multiply(BigInteger.valueOf(2));
        BigInteger v3 = BigInteger.ZERO;
        BigInteger v4 = BigInteger.ONE;
        BigInteger v5 = BigInteger.TEN;
        BigInteger v6 = BigInteger.valueOf(1000);
        BigInteger v7 = BigInteger.valueOf(10000000);

        check(input(v1, v2, v3, v4, v5, v6, v7), (v, out) -> out.writeBigInteger(v), DataReader::readBigInteger);
    }

    @Test
    public void testChar() throws Exception {
        check(input('a', '1'), (v, out) -> out.writeChar(v), DataInput::readChar);
    }

    @Test
    public void testUtfString() throws Exception {
        char[] chars = new char[100_000];

        Arrays.fill(chars, 'Ы');

        String longString = new String(chars);

        check(input("", "short string", longString), (v, out) -> out.writeUTF(v), DataInput::readUTF);
    }

    private <T> void check(T[] expected, WriterTask<T> write, ReaderTask read) throws IOException {
        for (T v : expected) {
            ByteBuf in = write(write, v);

            Object readResult = read(in, read);

            if (v.getClass().isArray() && v.getClass().getComponentType() == Byte.TYPE) {
                assertTrue(Arrays.equals((byte[])v, (byte[])readResult));
            } else {
                assertEquals(v, readResult);
            }
        }
    }

    private Object read(ByteBuf in, ReaderTask read) throws IOException {
        NettyMessage reader = new NettyMessage(in, fakeCodec);

        return read.read(reader);
    }

    @SafeVarargs
    private final <T> ByteBuf write(WriterTask<T> write, T... values) throws IOException {
        ByteBuf out = Unpooled.buffer();

        writer.setOut(out);

        for (T value : values) {
            write.write(value, writer);
        }

        return Unpooled.copiedBuffer(out);
    }

    @SafeVarargs
    private final <T> T[] input(T... values) {
        return values;
    }
}
