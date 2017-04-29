/*
 * Copyright 2017 The Hekate Project
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
        out.writeInt(address.getPort());
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
        int port = in.readInt();

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

            if (bits <= 63) {
                out.writeByte(DECIMAL_SMALL_UNSCALED);
                out.writeLong(val.longValue());
            } else {
                byte[] bytes = val.toByteArray();

                out.writeByte(DECIMAL_BIG);
                out.writeInt(bytes.length);
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
                long val = in.readLong();

                return BigInteger.valueOf(val);
            }
            case DECIMAL_BIG: {
                int bytesLen = in.readInt();

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

            if (bits <= 63) {
                if (scale == 0) {
                    out.writeByte(DECIMAL_SMALL_UNSCALED);
                    out.writeLong(unscaled.longValue());
                } else {
                    out.writeByte(DECIMAL_SMALL_SCALED);
                    out.writeInt(scale);
                    out.writeLong(unscaled.longValue());
                }
            } else {
                byte[] bytes = unscaled.toByteArray();

                out.writeByte(DECIMAL_BIG);
                out.writeInt(scale);
                out.writeInt(bytes.length);
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
                long val = in.readLong();

                return BigDecimal.valueOf(val);
            }
            case DECIMAL_SMALL_SCALED: {
                int scale = in.readInt();
                long unscaled = in.readLong();

                return BigDecimal.valueOf(unscaled, scale);
            }
            case DECIMAL_BIG: {
                int scale = in.readInt();
                int bytesLen = in.readInt();

                byte[] bytes = new byte[bytesLen];

                in.readFully(bytes);

                return new BigDecimal(new BigInteger(bytes), scale);
            }
            default: {
                throw new StreamCorruptedException("Unexpected hint for " + BigDecimal.class.getName() + " value [hint=" + hint + ']');
            }
        }
    }
}
