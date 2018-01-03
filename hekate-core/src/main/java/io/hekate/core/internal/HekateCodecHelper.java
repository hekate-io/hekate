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

package io.hekate.core.internal;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.core.Hekate;
import java.io.IOException;

final class HekateCodecHelper {
    private static final ThreadLocal<Hekate> INSTANCE = new ThreadLocal<>();

    private HekateCodecHelper() {
        // No-op.
    }

    public static Hekate threadLocal() {
        Hekate hekate = INSTANCE.get();

        if (hekate == null) {
            throw new IllegalStateException(Hekate.class.getSimpleName() + " is not associated with the current thread.");
        }

        return hekate;
    }

    public static <T> CodecFactory<T> wrap(CodecFactory<T> factory, Hekate instance) {
        return new CodecFactory<T>() {
            @Override
            public Codec<T> createCodec() {
                Codec<T> codec = factory.createCodec();

                return new Codec<T>() {
                    @Override
                    public boolean isStateful() {
                        return codec.isStateful();
                    }

                    @Override
                    public Class<T> baseType() {
                        return codec.baseType();
                    }

                    @Override
                    public T decode(DataReader in) throws IOException {
                        Hekate existing = INSTANCE.get();

                        boolean assign = existing == null;

                        if (assign) {
                            INSTANCE.set(instance);
                        }

                        try {
                            return codec.decode(in);
                        } finally {
                            if (assign) {
                                INSTANCE.remove();
                            }
                        }
                    }

                    @Override
                    public void encode(T message, DataWriter out) throws IOException {
                        codec.encode(message, out);
                    }

                    @Override
                    public String toString() {
                        return codec.toString();
                    }
                };
            }

            @Override
            public String toString() {
                return factory.toString();
            }
        };
    }
}
