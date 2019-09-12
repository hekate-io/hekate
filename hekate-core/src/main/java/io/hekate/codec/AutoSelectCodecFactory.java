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

import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.util.format.ToString;

/**
 * Codec factory that automatically selects an underlying implementation based on what is available on the classpath.
 *
 * <p>
 * This factory uses the following selection algorithm:
 * </p>
 * <ul>
 * <li>If Kryo is {@link #isKryoAvailable()  available} then {@link KryoCodecFactory} will be used</li>
 * <li>If FST is {@link #isFstAvailable() available} then {@link FstCodecFactory} will be used</li>
 * <li>otherwise {@link JdkCodecFactory} will be used</li>
 * </ul>
 *
 * @param <T> Base data type that is supported by the {@link Codec}.
 */
public class AutoSelectCodecFactory<T> implements CodecFactory<T> {
    private final CodecFactory<T> factory;

    /**
     * Constructs a new instance.
     */
    public AutoSelectCodecFactory() {
        if (isKryoAvailable()) {
            factory = new KryoCodecFactory<>();
        } else if (isFstAvailable()) {
            factory = new FstCodecFactory<>();
        } else {
            factory = new JdkCodecFactory<>();
        }
    }

    /**
     * Returns {@code true} if Kryo is available on the classpath.
     *
     * @return {@code true} if Kryo is available on the classpath.
     */
    public static boolean isKryoAvailable() {
        try {
            Class.forName("com.esotericsoftware.kryo.Kryo", false, Thread.currentThread().getContextClassLoader());

            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * Returns {@code true} if FST is available on the classpath.
     *
     * @return {@code true} if FST is available on the classpath.
     */
    public static boolean isFstAvailable() {
        try {
            Class.forName("org.nustaq.serialization.FSTConfiguration", false, Thread.currentThread().getContextClassLoader());

            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    /**
     * Returns the selected factory.
     *
     * @return Selected factory.
     */
    public CodecFactory<T> selected() {
        return factory;
    }

    @Override
    public Codec<T> createCodec() {
        return factory.createCodec();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
