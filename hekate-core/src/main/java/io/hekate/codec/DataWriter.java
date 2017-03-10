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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Data writer.
 *
 * @see Codec
 */
public interface DataWriter extends DataOutput {
    /**
     * Casts this writer to the {@link OutputStream} interface.
     *
     * @return This writer as {@link OutputStream}.
     */
    OutputStream asStream();

    /**
     * Writes {@link BigDecimal} value. The written value can be read via {@link DataReader#readBigDecimal()}.
     *
     * @param v Value.
     *
     * @throws IOException if failed to write value.
     */
    default void writeBigDecimal(BigDecimal v) throws IOException {
        CodecUtils.writeBigDecimal(v, this);
    }

    /**
     * Writer {@link BigInteger} value. The written value can be read via {@link DataReader#readBigInteger()}.
     *
     * @param v Value.
     *
     * @throws IOException if failed to write value.
     */
    default void writeBigInteger(BigInteger v) throws IOException {
        CodecUtils.writeBigInteger(v, this);
    }
}
