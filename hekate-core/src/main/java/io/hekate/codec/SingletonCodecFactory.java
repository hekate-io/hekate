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

/**
 * Singleton codec factory.
 *
 * <p>
 * This is a helper class that always returns the same {@link Codec} instance from the {@link #createCodec()} method. Can be used only with
 * those codecs that are safe to be shared by multiple threads.
 * </p>
 *
 * @param <T> Codec data type.
 */
public class SingletonCodecFactory<T> implements CodecFactory<T> {
    private final Codec<T> codec;

    /**
     * Constructs new instance.
     *
     * @param codec Singleton codec instance.
     */
    public SingletonCodecFactory(Codec<T> codec) {
        this.codec = codec;
    }

    @Override
    public Codec<T> createCodec() {
        return codec;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[codec=" + codec
            + ']';
    }
}
