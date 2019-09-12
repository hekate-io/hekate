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

package io.hekate.codec;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.DefaultClusterHash;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StreamCorruptedException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * Utilities for data encoding/decoding.
 */
public final class CodecUtils {

    private static final byte DECIMAL_ZERO = 1;

    private static final byte DECIMAL_ONE = 2;

    private static final byte DECIMAL_TEN = 3;

    private static final byte DECIMAL_SMALL_UNSCALED = 4;

    private static final byte DECIMAL_SMALL_SCALED = 5;

    private static final byte DECIMAL_BIG = 6;

    private static final int INT_BITS = 31;

    private static final int LONG_BITS = 63;

    private CodecUtils() {
        // No-op.
    }

    /**
     * Writes {@link ClusterAddress} to the specified data output.
     *
     * @param address Address to write.
     * @param out Data output.
     *
     * @throws IOException If write operation failed.
     * @see #readClusterAddress(DataInput)
     */
    public static void writeClusterAddress(ClusterAddress address, DataOutput out) throws IOException {
        writeNodeId(address.id(), out);

        writeAddress(address.socket(), out);
    }

    /**
     * Reads {@link ClusterAddress} from the specified data input.
     *
     * @param in Data input.
     *
     * @return Address.
     *
     * @throws IOException If read operation failed.
     * @see #writeClusterAddress(ClusterAddress, DataOutput)
     */
    public static ClusterAddress readClusterAddress(DataInput in) throws IOException {
        ClusterNodeId id = readNodeId(in);

        InetSocketAddress address = readAddress(in);

        return new ClusterAddress(address, id);
    }

    /**
     * Writes {@link InetSocketAddress} to the specified data output.
     *
     * @param address Address to write.
     * @param out Data output.
     *
     * @throws IOException If write operation failed.
     * @see #readAddress(DataInput)
     */
    public static void writeAddress(InetSocketAddress address, DataOutput out) throws IOException {
        // Address host.
        byte[] hostBytes = address.getAddress().getAddress();

        out.writeByte(hostBytes.length);
        out.write(hostBytes);

        // Address port.
        writeVarInt(address.getPort(), out);
    }

    /**
     * Reads {@link InetSocketAddress} from the specified data input.
     *
     * @param in Data input.
     *
     * @return Address.
     *
     * @throws IOException If read operation failed.
     * @see #writeAddress(InetSocketAddress, DataOutput)
     */
    public static InetSocketAddress readAddress(DataInput in) throws IOException {
        // Host.
        byte[] hostBytes = new byte[in.readByte()];

        in.readFully(hostBytes);

        InetAddress host = InetAddress.getByAddress(hostBytes);

        // Port.
        int port = readVarInt(in);

        return new InetSocketAddress(host, port);
    }

    /**
     * Writes {@link ClusterNodeId} to the specified data output.
     *
     * @param id Node ID.
     * @param out Data output.
     *
     * @throws IOException If write operation failed.
     * @see #readNodeId(DataInput)
     */
    public static void writeNodeId(ClusterNodeId id, DataOutput out) throws IOException {
        out.writeLong(id.hiBits());
        out.writeLong(id.loBits());
    }

    /**
     * Reads {@link ClusterNodeId} from the specified data input.
     *
     * @param in Data input.
     *
     * @return Node ID.
     *
     * @throws IOException If read operation failed.
     * @see #writeNodeId(ClusterNodeId, DataOutput)
     */
    public static ClusterNodeId readNodeId(DataInput in) throws IOException {
        return new ClusterNodeId(in.readLong(), in.readLong());
    }

    /**
     * Writes {@link ClusterHash} to the specified data output.
     *
     * @param hash Topology hash.
     * @param out Data output.
     *
     * @throws IOException If write operation failed.
     * @see #readTopologyHash(DataInput)
     */
    public static void writeTopologyHash(ClusterHash hash, DataOutput out) throws IOException {
        byte[] bytes = hash.bytes();

        out.writeByte(bytes.length);
        out.write(bytes);
    }

    /**
     * Reads {@link ClusterHash} from the specified data input.
     *
     * @param in Data input.
     *
     * @return Topology hash.
     *
     * @throws IOException If read operation failed.
     * @see #writeTopologyHash(ClusterHash, DataOutput)
     */
    public static ClusterHash readTopologyHash(DataInput in) throws IOException {
        byte[] bytes = new byte[in.readByte()];

        in.readFully(bytes);

        return new DefaultClusterHash(bytes);
    }

    /**
     * Writes {@link BigInteger} value. The written value can be read via {@link #readBigInteger(DataInput)}.
     *
     * @param val Value.
     * @param out Data output.
     *
     * @throws IOException if failed to write value.
     */
    public static void writeBigInteger(BigInteger val, DataOutput out) throws IOException {
        if (val.equals(BigInteger.ZERO)) {
            out.writeByte(DECIMAL_ZERO);
        } else if (val.equals(BigInteger.ONE)) {
            out.writeByte(DECIMAL_ONE);
        } else if (val.equals(BigInteger.TEN)) {
            out.writeByte(DECIMAL_TEN);
        } else {
            int bits = val.bitLength();

            if (bits <= LONG_BITS) {
                out.writeByte(DECIMAL_SMALL_UNSCALED);

                writeVarLong(val.longValue(), out);
            } else {
                byte[] bytes = val.toByteArray();

                out.writeByte(DECIMAL_BIG);
                writeVarIntUnsigned(bytes.length, out);

                out.write(bytes, 0, bytes.length);
            }

        }
    }

    /**
     * Reads {@link BigInteger} value that was written via {@link #writeBigInteger(BigInteger, DataOutput)}.
     *
     * @param in Data input.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    public static BigInteger readBigInteger(DataInput in) throws IOException {
        byte hint = in.readByte();

        switch (hint) {
            case DECIMAL_ZERO: {
                return BigInteger.ZERO;
            }
            case DECIMAL_ONE: {
                return BigInteger.ONE;
            }
            case DECIMAL_TEN: {
                return BigInteger.TEN;
            }
            case DECIMAL_SMALL_UNSCALED: {
                long val = readVarLong(in);

                return BigInteger.valueOf(val);
            }
            case DECIMAL_BIG: {
                int bytesLen = readVarIntUnsigned(in);

                byte[] bytes = new byte[bytesLen];

                in.readFully(bytes);

                return new BigInteger(bytes);
            }
            default: {
                throw new StreamCorruptedException("Unexpected hint for " + BigInteger.class.getName() + " value [hint=" + hint + ']');
            }
        }
    }

    /**
     * Writes {@link BigDecimal} value. The written value can be read via {@link #readBigDecimal(DataInput)}.
     *
     * @param val Value.
     * @param out Data output.
     *
     * @throws IOException if failed to write value.
     */
    public static void writeBigDecimal(BigDecimal val, DataOutput out) throws IOException {
        if (val.compareTo(BigDecimal.ZERO) == 0) {
            out.writeByte(DECIMAL_ZERO);
        } else if (val.compareTo(BigDecimal.ONE) == 0) {
            out.writeByte(DECIMAL_ONE);
        } else if (val.compareTo(BigDecimal.TEN) == 0) {
            out.writeByte(DECIMAL_TEN);
        } else {
            int scale = val.scale();

            BigInteger unscaled = val.unscaledValue();

            int bits = unscaled.bitLength();

            if (bits <= LONG_BITS) {
                if (scale == 0) {
                    out.writeByte(DECIMAL_SMALL_UNSCALED);
                } else {
                    out.writeByte(DECIMAL_SMALL_SCALED);
                    writeVarIntUnsigned(scale, out);
                }

                writeVarLong(unscaled.longValue(), out);
            } else {
                byte[] bytes = unscaled.toByteArray();

                out.writeByte(DECIMAL_BIG);
                writeVarIntUnsigned(scale, out);
                writeVarIntUnsigned(bytes.length, out);
                out.write(bytes, 0, bytes.length);
            }
        }
    }

    /**
     * Reads {@link BigDecimal} value that was written via {@link #writeBigDecimal(BigDecimal, DataOutput)}.
     *
     * @param in Data input.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    public static BigDecimal readBigDecimal(DataInput in) throws IOException {
        byte hint = in.readByte();

        switch (hint) {
            case DECIMAL_ZERO: {
                return BigDecimal.ZERO;
            }
            case DECIMAL_ONE: {
                return BigDecimal.ONE;
            }
            case DECIMAL_TEN: {
                return BigDecimal.TEN;
            }
            case DECIMAL_SMALL_UNSCALED: {
                long val = readVarLong(in);

                return BigDecimal.valueOf(val);
            }
            case DECIMAL_SMALL_SCALED: {
                int scale = readVarIntUnsigned(in);
                long unscaled = readVarLong(in);

                return BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL_BIG: {
                int scale = readVarIntUnsigned(in);
                int bytesLen = readVarIntUnsigned(in);

                byte[] bytes = new byte[bytesLen];

                in.readFully(bytes);

                return new BigDecimal(new BigInteger(bytes), scale);
            }
            default: {
                throw new StreamCorruptedException("Unexpected hint for " + BigDecimal.class.getName() + " value [hint=" + hint + ']');
            }
        }
    }

    /**
     * Encodes a value using the variable-length encoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently encode signed values. If values are known to be non-negative,
     * {@link #writeVarLongUnsigned(long, DataOutput)} should be used.
     *
     * @param value Value to encode
     * @param out Data output.
     *
     * @throws IOException if failed to write value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static void writeVarLong(long value, DataOutput out) throws IOException {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeVarLongUnsigned(value << 1 ^ value >> LONG_BITS, out);
    }

    /**
     * Encodes a value using the variable-length encoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative. If values can be negative, use {@link
     * #writeVarLong(long, DataOutput)} instead. This method treats negative input as like a large unsigned value.
     *
     * @param value Value to encode (must be non-negative).
     * @param out Data output.
     *
     * @throws IOException if failed to write value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static void writeVarLongUnsigned(long value, DataOutput out) throws IOException {
        while ((value & 0xFFFFFFFFFFFFFF80L) != 0L) {
            out.writeByte(((int)value & 0x7F) | 0x80);

            value >>>= 7;
        }

        out.writeByte((int)value & 0x7F);
    }

    /**
     * Encodes a value using the variable-length encoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. It uses zig-zag encoding to efficiently encode signed values. If values are known to be non-negative,
     * {@link #writeVarIntUnsigned(int, DataOutput)}  should be used.
     *
     * @param value Value to encode
     * @param out Data output.
     *
     * @throws IOException if failed to write value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static void writeVarInt(int value, DataOutput out) throws IOException {
        // Great trick from http://code.google.com/apis/protocolbuffers/docs/encoding.html#types
        writeVarIntUnsigned(value << 1 ^ value >> INT_BITS, out);
    }

    /**
     * Encodes a value using the variable-length encoding from <a href="http://code.google.com/apis/protocolbuffers/docs/encoding.html">
     * Google Protocol Buffers</a>. Zig-zag is not used, so input must not be negative. If values can be negative, use {@link
     * #writeVarInt(int, DataOutput)} instead. This method treats negative input as like a large unsigned value.
     *
     * @param value Value to encode (must be non-negative).
     * @param out Data output.
     *
     * @throws IOException if failed to write value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static void writeVarIntUnsigned(int value, DataOutput out) throws IOException {
        while ((value & 0xFFFFFF80) != 0L) {
            out.writeByte((value & 0x7F) | 0x80);

            value >>>= 7;
        }

        out.writeByte(value & 0x7F);
    }

    /**
     * Reads a value that was encoded via {@link #writeVarLong(long, DataOutput)}.
     *
     * @param in Data input.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static long readVarLong(DataInput in) throws IOException {
        long raw = readVarLongUnsigned(in);
        // This undoes the trick in writeVarLong()
        long temp = (raw << LONG_BITS >> LONG_BITS ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & 1L << LONG_BITS);
    }

    /**
     * Reads a value that was encoded via {@link #writeVarLongUnsigned(long, DataOutput)}.
     *
     * @param in Data input.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static long readVarLongUnsigned(DataInput in) throws IOException {
        long value = 0L;
        int i = 0;
        long b;

        while (((b = in.readByte()) & 0x80L) != 0) {
            value |= (b & 0x7F) << i;

            i += 7;

            if (i > LONG_BITS) {
                throw new StreamCorruptedException("Variable length size is too long");
            }
        }

        return value | b << i;
    }

    /**
     * Reads a value that was encoded via {@link #writeVarInt(int, DataOutput)}.
     *
     * @param in Data input.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static int readVarInt(DataInput in) throws IOException {
        int raw = readVarIntUnsigned(in);
        // This undoes the trick in writeVarInt()
        int temp = (raw << INT_BITS >> INT_BITS ^ raw) >> 1;
        // This extra step lets us deal with the largest signed values by treating
        // negative results from read unsigned methods as like unsigned values.
        // Must re-flip the top bit if the original read value had it set.
        return temp ^ (raw & 1 << INT_BITS);
    }

    /**
     * Reads a value that was encoded via {@link #writeVarIntUnsigned(int, DataOutput)}.
     *
     * @param in Data input.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    // Code borrowed from 'stream-lib' (Apache 2.0 license) - see https://github.com/addthis/stream-lib
    public static int readVarIntUnsigned(DataInput in) throws IOException {
        int value = 0;
        int i = 0;
        int b;

        while (((b = in.readByte()) & 0x80) != 0) {
            value |= (b & 0x7F) << i;

            i += 7;

            if (i > INT_BITS) {
                throw new StreamCorruptedException("Variable length size is too long");
            }
        }

        return value | b << i;
    }
}
