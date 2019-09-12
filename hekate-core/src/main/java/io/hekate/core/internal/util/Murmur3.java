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

package io.hekate.core.internal.util;

/**
 * Copy of MurmurHash3 code from Google Guava library (Licensed under the Apache License, Version 2.0).
 *
 * <p>
 * Guava Project Page: <a href="https://github.com/google/guava">https://github.com/google/guava</a>
 * </p>
 *
 * <p>
 * MurmurHash3 was written by Austin Appleby, and is placed in the public
 * domain. The author hereby disclaims copyright to this
 * <a href="https://github.com/aappleby/smhasher/blob/master/src/MurmurHash3.cpp">source code</a>.
 * </p>
 */
public final class Murmur3 {
    private static final int C1 = 0xcc9e2d51;

    private static final int C2 = 0x1b873593;

    private Murmur3() {
        // No-op.
    }

    public static int hash(int n1, int n2) {
        int k1 = mixK1(n1);
        int h1 = mixH1(0, k1);

        k1 = mixK1(n2);
        h1 = mixH1(h1, k1);

        return mix(h1, Integer.BYTES);
    }

    private static int mixK1(int k1) {
        k1 *= C1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= C2;

        return k1;
    }

    private static int mixH1(int h1, int k1) {
        h1 ^= k1;
        h1 = Integer.rotateLeft(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;

        return h1;
    }

    private static int mix(int h1, int length) {
        h1 ^= length;
        h1 ^= h1 >>> 16;
        h1 *= 0x85ebca6b;
        h1 ^= h1 >>> 13;
        h1 *= 0xc2b2ae35;
        h1 ^= h1 >>> 16;

        return h1;
    }
}
