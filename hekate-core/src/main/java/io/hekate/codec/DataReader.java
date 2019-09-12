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

import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Data reader.
 *
 * @see Codec
 */
public interface DataReader extends DataInput {
    /**
     * Casts this writer to the {@link InputStream} interface.
     *
     * @return This writer as {@link InputStream}.
     */
    InputStream asStream();

    /**
     * Reads {@link BigDecimal} value that wa written via {@link DataWriter#writeBigDecimal(BigDecimal)}.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    default BigDecimal readBigDecimal() throws IOException {
        return CodecUtils.readBigDecimal(this);
    }

    /**
     * Reads {@link BigInteger} value that was written via {@link DataWriter#writeBigInteger(BigInteger)}.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    default BigInteger readBigInteger() throws IOException {
        return CodecUtils.readBigInteger(this);
    }

    /**
     * Reads a variable-length {@code int} value that was written via {@link DataWriter#writeVarInt(int)}.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    default int readVarInt() throws IOException {
        return CodecUtils.readVarInt(this);
    }

    /**
     * Reads an unsigned variable-length {@code int} value that was written via {@link DataWriter#writeVarIntUnsigned(int)}.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    default int readVarIntUnsigned() throws IOException {
        return CodecUtils.readVarIntUnsigned(this);
    }

    /**
     * Reads a variable-length {@code long} value that was written via {@link DataWriter#writeVarLong(long)}.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    default long readVarLong() throws IOException {
        return CodecUtils.readVarLong(this);
    }

    /**
     * Reads an unsigned variable-length {@code long} value that was written via {@link DataWriter#writeVarLongUnsigned(long)}.
     *
     * @return Value.
     *
     * @throws IOException if failed to read value.
     */
    default long readVarLongUnsigned() throws IOException {
        return CodecUtils.readVarLongUnsigned(this);
    }
}
