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

package io.hekate.codec.kryo;

import com.esotericsoftware.kryo.ClassResolver;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.core.HekateBootstrap;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.HashMap;
import java.util.Map;
import org.objenesis.strategy.InstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * <span class="startHere">&laquo; start here</span><a href="https://github.com/EsotericSoftware/kryo" target="_blank">Kryo</a>-based
 * implementation of {@link CodecFactory} interface.
 *
 * <h2>Module dependency</h2>
 * <p>
 * Kryo integration requires
 * <a href="https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.esotericsoftware%22%20a%3A%22kryo-shaded%22" target="_blank">
 * 'com.esotericsoftware:kryo-shaded'
 * </a>
 * to be on the project's classpath.
 * </p>
 *
 * <p>
 * It is also recommended to add
 * <a href="https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22de.javakaffee%22%20a%3A%22kryo-serializers%22" target="_blank">
 * 'de.javakaffee:kryo-serializers'
 * </a> as it provides extended support for some of the JDK's built-in types as well as for some popular third-party libraries.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * Each call of {@link #createCodec()} method will produces a new independent {@link Kryo} instance and will configure it
 * according to properties of this factory (see description of property setters).
 * ${source: codec/kryo/KryoCodecFactoryJavadocTest.java#configuration}
 * </p>
 *
 * @param <T> Base type of data that should be supported by this factory.
 *
 * @see HekateBootstrap#setDefaultCodec(CodecFactory)
 */
public class KryoCodecFactory<T> implements CodecFactory<T> {
    private Map<Integer, Class<?>> knownTypes;

    private Map<Class<?>, Serializer<?>> serializers;

    private Map<Class<?>, Serializer<?>> defaultSerializers;

    private boolean unsafeIo = true;

    private Boolean references;

    private boolean cacheUnknownTypes;

    @ToStringIgnore
    private InstantiatorStrategy instantiatorStrategy = new DefaultInstantiatorStrategy(new StdInstantiatorStrategy());

    @Override
    @SuppressWarnings("unchecked")
    public Codec<T> createCodec() {
        return (Codec<T>)new KryoCodec(this);
    }

    /**
     * Returns the map of known java types and their IDs (see {@link #setKnownTypes(Map)}).
     *
     * @return Map of known types and their codes.
     */
    public Map<Integer, Class<?>> getKnownTypes() {
        return knownTypes;
    }

    /**
     * Sets the map of known java types and their IDs. Such types will be registered via {@link Kryo#register(Class, int)}.
     *
     * @param knownTypes Map of known java types and their IDs.
     */
    public void setKnownTypes(Map<Integer, Class<?>> knownTypes) {
        this.knownTypes = knownTypes;
    }

    /**
     * Fluent-style version of {@link #setKnownTypes(Map)}.
     *
     * @param id Type ID.
     * @param type Type.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withKnownType(int id, Class<?> type) {
        if (knownTypes == null) {
            knownTypes = new HashMap<>();
        }

        knownTypes.put(id, type);

        return this;
    }

    /**
     * Fluent-style version of {@link #setKnownTypes(Map)}.
     *
     * @param types Map of known java types and their IDs.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withKnownTypes(Map<Integer, Class<?>> types) {
        if (knownTypes == null) {
            knownTypes = new HashMap<>();
        }

        knownTypes.putAll(types);

        return this;
    }

    /**
     * Returns the map of custom serializer (see {@link #setSerializers(Map)}).
     *
     * @return Map of custom serializer.
     */
    public Map<Class<?>, Serializer<?>> getSerializers() {
        return serializers;
    }

    /**
     * Sets the map of custom serializers. Such serializers will be registered via {@link Kryo#register(Class, Serializer)}.
     *
     * @param serializers Map of serializers.
     */
    public void setSerializers(Map<Class<?>, Serializer<?>> serializers) {
        this.serializers = serializers;
    }

    /**
     * Fluent-style version of {@link #setSerializers(Map)}.
     *
     * @param type Type.
     * @param serializer Type serializer.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withSerializer(Class<?> type, Serializer<?> serializer) {
        if (serializers == null) {
            serializers = new HashMap<>();
        }

        serializers.put(type, serializer);

        return this;
    }

    /**
     * Returns the map of default serializers (see {@link #setSerializers(Map)}).
     *
     * @return Map of default serializer.
     */
    public Map<Class<?>, Serializer<?>> getDefaultSerializers() {
        return defaultSerializers;
    }

    /**
     * Sets the map of default serializer. Such serializers will be registered via {@link Kryo#addDefaultSerializer(Class, Serializer)}.
     *
     * @param defaultSerializers Map of default serializer.
     */
    public void setDefaultSerializers(Map<Class<?>, Serializer<?>> defaultSerializers) {
        this.defaultSerializers = defaultSerializers;
    }

    /**
     * Fluent style version of {@link #setDefaultSerializers(Map)}.
     *
     * @param type Type to serialize.
     * @param serializer Serializer.
     * @param <V> Type to serialize.
     *
     * @return This instance.
     */
    public <V> KryoCodecFactory<T> withDefaultSerializer(Class<V> type, Serializer<V> serializer) {
        if (defaultSerializers == null) {
            defaultSerializers = new HashMap<>();
        }

        defaultSerializers.put(type, serializer);

        return this;
    }

    /**
     * Returns the instantiator strategy (see {@link #setInstantiatorStrategy(InstantiatorStrategy)}).
     *
     * @return Instantiator strategy.
     */
    public InstantiatorStrategy getInstantiatorStrategy() {
        return instantiatorStrategy;
    }

    /**
     * Sets instantiator strategy. Such strategy will be registered via {@link Kryo#setInstantiatorStrategy(InstantiatorStrategy)}.
     *
     * <p>
     * By default this parameter is set to {@link DefaultInstantiatorStrategy} with a fallback to {@link StdInstantiatorStrategy}.
     * </p>
     *
     * @param instantiatorStrategy Instantiator strategy.
     */
    public void setInstantiatorStrategy(InstantiatorStrategy instantiatorStrategy) {
        this.instantiatorStrategy = instantiatorStrategy;
    }

    /**
     * Fluent-style version of {@link #setInstantiatorStrategy(InstantiatorStrategy)}.
     *
     * @param instantiatorStrategy Instantiator strategy.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withInstantiatorStrategy(InstantiatorStrategy instantiatorStrategy) {
        setInstantiatorStrategy(instantiatorStrategy);

        return this;
    }

    /**
     * Returns {@code true} if {@code Unsafe}-based IO should be used by Kryo (see {@link #setUnsafeIo(boolean)}).
     *
     * @return {@code true} if {@code Unsafe}-based IO should be used by Kryo.
     */
    public boolean isUnsafeIo() {
        return unsafeIo;
    }

    /**
     * Set to {@code true} in order to use {@link UnsafeInput}/{@link UnsafeOutput} with Kryo.
     *
     * <p>
     * Default value of this parameter is {@code true}.
     * </p>
     *
     * @param unsafeIo {@code true} to enable {@code Unsafe}-based IO.
     */
    public void setUnsafeIo(boolean unsafeIo) {
        this.unsafeIo = unsafeIo;
    }

    /**
     * Fluent-style version of {@link #setUnsafeIo(boolean)}.
     *
     * @param unsafeIo {@code true} to enable {@code Unsafe}-based IO.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withUnsafeIo(boolean unsafeIo) {
        setUnsafeIo(unsafeIo);

        return this;
    }

    /**
     * Returns the value that should override {@link Kryo#setReferences(boolean)} flag (see {@link #setReferences(Boolean)}).
     *
     * @return Value that should override {@link Kryo#setReferences(boolean)} flag.
     */
    public Boolean getReferences() {
        return references;
    }

    /**
     * Sets the value that should override the {@link Kryo#setReferences(boolean)} flag. If value of this parameter is {@code null}
     * (default) then {@link Kryo#setReferences(boolean)} will not be called during Kryo instance construction.
     *
     * @param references Value that should override the {@link Kryo#setReferences(boolean)} flag.
     */
    public void setReferences(Boolean references) {
        this.references = references;
    }

    /**
     * Fluent-style version of {@link #setReferences(Boolean)}.
     *
     * @param references Value that should override the {@link Kryo#setReferences(boolean)} flag.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withReferences(Boolean references) {
        setReferences(references);

        return this;
    }

    /**
     * Returns the flag that controls whether class-to-identifiers mapping should be cached (see {@link #setCacheUnknownTypes(boolean)}).
     *
     * @return Flag that controls whether class-to-identifiers mapping should be cached.
     */
    public boolean isCacheUnknownTypes() {
        return cacheUnknownTypes;
    }

    /**
     * Sets the flag that controls the behavior of unmapped classes caching.
     *
     * <p>
     * If set to {@code true} then mapping of unknown classes (that were not explicitly registered via {@link #withKnownType(int, Class)})
     * to their generated identifiers will be preserved across multiple calls of {@link Codec#encode(Object, DataWriter) encode}/{@link
     * Codec#decode(DataReader) decode} methods when called on the same {@link Codec} instance. If set to {@code false} then such
     * mapping will be reset (see {@link ClassResolver#reset()}) after each {@link Codec#encode(Object, DataWriter) encode}/{@link
     * Codec#decode(DataReader) decode} operation.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code false}.
     * </p>
     *
     * @param cacheUnknownTypes Flag that controls whether class-to-identifiers mapping should be cached.
     */
    public void setCacheUnknownTypes(boolean cacheUnknownTypes) {
        this.cacheUnknownTypes = cacheUnknownTypes;
    }

    /**
     * Fluent-style version of {@link #setCacheUnknownTypes(boolean)}.
     *
     * @param cacheUnknownTypes Flag that controls whether class-to-identifiers mapping should be cached.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withCacheUnknownTypes(boolean cacheUnknownTypes) {
        setCacheUnknownTypes(cacheUnknownTypes);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
