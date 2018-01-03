/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.messaging.internal;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;

class ThreadLocalCodecFactory<T> implements CodecFactory<T> {
    private final CodecFactory<T> delegate;

    private final Class<T> baseType;

    @ToStringIgnore
    private final ThreadLocal<Codec<T>> cache = new ThreadLocal<>();

    public ThreadLocalCodecFactory(CodecFactory<T> delegate) {
        this.delegate = delegate;

        baseType = delegate.createCodec().baseType();
    }

    @Override
    public Codec<T> createCodec() {
        return new Codec<T>() {
            @Override
            public boolean isStateful() {
                return false;
            }

            @Override
            public Class<T> baseType() {
                return baseType;
            }

            @Override
            public T decode(DataReader in) throws IOException {
                Codec<T> codec = cache.get();

                if (codec == null) {
                    codec = delegate.createCodec();

                    cache.set(codec);
                }

                return codec.decode(in);
            }

            @Override
            public void encode(T obj, DataWriter out) throws IOException {
                Codec<T> codec = cache.get();

                if (codec == null) {
                    codec = delegate.createCodec();

                    cache.set(codec);
                }

                codec.encode(obj, out);
            }

            @Override
            public String toString() {
                return delegate.toString();
            }
        };
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
