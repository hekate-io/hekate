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
 * Codec factory that uses the standard Java Serialization API for data encoding/decoding.
 *
 * @param <T> Base type of object that should be supported by the codec (can be {@link Object}).
 */
public class JdkCodecFactory<T> implements CodecFactory<T> {
    private static final JdkCodec CODEC = new JdkCodec();

    @Override
    @SuppressWarnings("unchecked")
    public Codec<T> createCodec() {
        return (Codec<T>)CODEC;
    }

    @Override
    public String toString() {
        return JdkCodecFactory.class.getSimpleName();
    }
}
