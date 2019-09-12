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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;

/**
 * Codec factory that manages a per-thread cache of {@link Codec}s.
 *
 * @param <T> Base data type of this factory.
 */
public final class ThreadLocalCodecFactory<T> implements CodecFactory<T> {
    private final CodecFactory<T> delegate;

    @ToStringIgnore
    // Notice! Non-static because of we need to store a thread-local cache per each factory.
    private final ThreadLocal<Codec<T>> cache = new ThreadLocal<>();

    @ToStringIgnore
    private final Codec<T> threadLocalCodec;

    private ThreadLocalCodecFactory(Class<T> baseType, CodecFactory<T> delegate) {
        this.delegate = delegate;

        this.threadLocalCodec = new Codec<T>() {
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

    /**
     * Tries to wrap the specified factory with {@link ThreadLocalCodecFactory}.
     *
     * <p>
     * If the specified factory is {@link Codec#isStateful() stateful} or if it is already an instance of {@link ThreadLocalCodecFactory}
     * then it will not be wrapped. In such case the value returned by this method will be the same object that was passed in as a
     * parameter.
     * </p>
     *
     * @param factory Factory to warp.
     * @param <T> Factory type.
     *
     * @return Wrapped instance or the factory instance that was passed in as a parameter
     * if such factory is {@link Codec#isStateful() stateful} or if it is already an instance of {@link ThreadLocalCodecFactory}.
     */
    public static <T> CodecFactory<T> tryWrap(CodecFactory<T> factory) {
        ArgAssert.notNull(factory, "Codec factory is null.");

        if (factory instanceof ThreadLocalCodecFactory) {
            return factory;
        }

        Codec<T> probe = factory.createCodec();

        if (probe.isStateful()) {
            return factory;
        } else {
            return new ThreadLocalCodecFactory<>(probe.baseType(), factory);
        }
    }

    /**
     * Tries to unwrap the specified factory.
     *
     * <p>
     * If the specified factory is an instance of {@link ThreadLocalCodecFactory} then a factory that is wrapped by that
     * {@link ThreadLocalCodecFactory} is returned; otherwise the method's parameter is returned as is.
     * </p>
     *
     * @param factory Factory to unwrap.
     * @param <T> Factory data type.
     *
     * @return Unwrapped codec factory if the specified parameter is of {@link ThreadLocalCodecFactory} type; otherwise returns the method's
     * parameter as is.
     */
    @SuppressWarnings("unchecked")
    public static <T> CodecFactory<T> tryUnwrap(CodecFactory<T> factory) {
        if (factory instanceof ThreadLocalCodecFactory) {
            return ((ThreadLocalCodecFactory)factory).delegate;
        }

        return factory;
    }

    @Override
    public Codec<T> createCodec() {
        return threadLocalCodec;
    }

    @Override
    public String toString() {
        return ThreadLocalCodecFactory.class.getSimpleName() + "[delegate=" + delegate + ']';
    }
}
