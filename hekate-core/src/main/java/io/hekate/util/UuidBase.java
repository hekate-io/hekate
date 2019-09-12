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

package io.hekate.util;

import java.io.Serializable;
import java.util.UUID;

/**
 * Base class for custom UUID-based identifiers.
 *
 * @param <T> Identifier type.
 */
public abstract class UuidBase<T extends UuidBase<T>> implements Serializable, Comparable<T> {
    private static final long serialVersionUID = 1;

    private final long hiBits;

    private final long loBits;

    /**
     * Constructs new random identifier.
     */
    public UuidBase() {
        UUID uuid = UUID.randomUUID();

        hiBits = uuid.getMostSignificantBits();
        loBits = uuid.getLeastSignificantBits();
    }

    /**
     * Constructs new instance from the specified higher/lower bits.
     *
     * @param hiBits Higher bits (see {@link #hiBits()}).
     * @param loBits Lower bits (see {@link #loBits()}).
     */
    public UuidBase(long hiBits, long loBits) {
        this.hiBits = hiBits;
        this.loBits = loBits;
    }

    /**
     * Creates new identifier from the specified string.
     *
     * <p>
     * Only strings that were produced by the {@link #toString()} method can be parsed.
     * </p>
     *
     * @param s String (see {@link #toString()}).
     */
    public UuidBase(String s) {
        String timeLow = "0x" + s.substring(0, 8);
        String timeMid = "0x" + s.substring(8, 12);
        String timeHighAndVersion = "0x" + s.substring(12, 16);

        // High bits.
        long hiBits = Long.decode(timeLow);
        hiBits <<= 16;
        hiBits |= Long.decode(timeMid);
        hiBits <<= 16;
        hiBits |= Long.decode(timeHighAndVersion);

        // Low bits.
        String variantAndSequence = "0x" + s.substring(16, 20);
        String node = "0x" + s.substring(20, 32);

        long loBits = Long.decode(variantAndSequence);
        loBits <<= 48;
        loBits |= Long.decode(node);

        this.hiBits = hiBits;
        this.loBits = loBits;
    }

    /**
     * Returns higher bits of this identifier.
     *
     * <p>
     * This property can be used for custom serialization of this identifier. Value of this property can be used with {@link
     * #UuidBase(long, long)} to reconstruct this identifier.
     * </p>
     *
     * @return Higher bits of this identifier.
     */
    public long hiBits() {
        return hiBits;
    }

    /**
     * Returns lower bits of this identifier.
     *
     * <p>
     * This property can be used for custom serialization of this identifier. Value of this property can be used with {@link
     * #UuidBase(long, long)} to reconstruct this identifier.
     * </p>
     *
     * @return Lower bits of this identifier.
     */
    public long loBits() {
        return loBits;
    }

    @Override
    public int compareTo(T other) {
        long hiBits2 = other.hiBits();
        long loBits2 = other.loBits();

        if (hiBits < hiBits2) {
            return -1;
        } else if (hiBits > hiBits2) {
            return 1;
        } else {
            return Long.compare(loBits, loBits2);
        }
    }

    private static String digits(long val, int digits) {
        long hi = 1L << (digits * 4);

        return Long.toHexString(hi | (val & hi - 1)).substring(1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        @SuppressWarnings("unchecked")
        UuidBase<T> that = (UuidBase<T>)o;

        return hiBits == that.hiBits && loBits == that.loBits;
    }

    @Override
    public int hashCode() {
        long hiLo = hiBits ^ loBits;

        return (int)(hiLo >> 32) ^ (int)hiLo;
    }

    @Override
    public String toString() {
        return digits(hiBits >> 32, 8)
            + digits(hiBits >> 16, 4)
            + digits(hiBits, 4)
            + digits(loBits >> 48, 4)
            + digits(loBits, 12);
    }
}
