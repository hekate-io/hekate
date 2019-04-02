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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.objenesis.strategy.InstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * <span class="startHere">&laquo; start here</span><a href="https://github.com/EsotericSoftware/kryo" target="_blank">Kryo</a>-based
 * implementation of {@link CodecFactory} interface.
 *
 * <h2>Module dependency</h2>
 * <p>
 * Kryo integration is provided by the 'hekate-codec-kryo' module and can be imported into the project dependency management system as
 * in the example below:
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#maven">Maven</a></li>
 * <li><a href="#gradle">Gradle</a></li>
 * <li><a href="#ivy">Ivy</a></li>
 * </ul>
 * <div id="maven">
 * <pre>{@code
 * <dependency>
 *   <groupId>io.hekate</groupId>
 *   <artifactId>hekate-codec-kryo</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-codec-kryo', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-codec-kryo" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
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
    /** See {@link #setKnownTypes(List)}. */
    private List<Class<?>> knownTypes;

    /** See {@link #setRegistrationRequired(boolean)}. */
    private boolean registrationRequired;

    /** See {@link #setSerializers(Map)}. */
    private Map<Class<?>, Serializer<?>> serializers;

    /** See {@link #setDefaultSerializers(Map)}. */
    private Map<Class<?>, Serializer<?>> defaultSerializers;

    /** See {@link #setUnsafeIo(boolean)}. */
    private boolean unsafeIo = true;

    /** See {@link #setReferences(Boolean)}. */
    private Boolean references;

    /** See {@link #setCacheUnknownTypes(boolean)}. */
    private boolean cacheUnknownTypes;

    /** See {@link #setInstantiatorStrategy(InstantiatorStrategy)}. */
    @ToStringIgnore
    private InstantiatorStrategy instantiatorStrategy = new DefaultInstantiatorStrategy(new StdInstantiatorStrategy());

    @Override
    @SuppressWarnings("unchecked")
    public Codec<T> createCodec() {
        return (Codec<T>)new KryoCodec(this);
    }

    /**
     * Returns the list of known java types (see {@link #setKnownTypes(List)}).
     *
     * @return List of known types.
     */
    public List<Class<?>> getKnownTypes() {
        return knownTypes;
    }

    /**
     * Sets the list of known Java types. Such types will be registered via {@link Kryo#register(Class)}.
     *
     * <p>
     * <b>Notice:</b>
     * Exactly the same list of types should be registered on all cluster nodes. Otherwise Kryo will not be able deserialize data.
     * Fore more details please see {@link Kryo#register(Class)}.
     * </p>
     *
     * @param knownTypes List of known Java types.
     */
    public void setKnownTypes(List<Class<?>> knownTypes) {
        this.knownTypes = knownTypes;
    }

    /**
     * Fluent-style version of {@link #setKnownTypes(List)}.
     *
     * @param type Type.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withKnownType(Class<?> type) {
        if (knownTypes == null) {
            knownTypes = new ArrayList<>();
        }

        knownTypes.add(type);

        return this;
    }

    /**
     * Fluent-style version of {@link #setKnownTypes(List)}.
     *
     * @param types List of known Java types.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withKnownTypes(List<Class<?>> types) {
        if (knownTypes == null) {
            knownTypes = new ArrayList<>();
        }

        knownTypes.addAll(types);

        return this;
    }

    /**
     * Returns the flag indicating if registration is required for all serializable classes (see {@link #setRegistrationRequired(boolean)}).
     *
     * @return Flag indicating if registration is required for all serializable classes.
     */
    public boolean isRegistrationRequired() {
        return registrationRequired;
    }

    /**
     * Sets the flag indicating that all serializable classes must be registered via {@link #setKnownTypes(List)}.
     *
     * <p>
     * In order to get the best performance of data serialization it is recommended to register all serializable classes in advance.
     * If this flag is set to {@code true} then Kryo will throw an error when trying to serialize a class that is not {@link
     * #setKnownTypes(List) registered}. However sometimes it is not feasible to do so, in such case this flag should be set {@code false}.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code false} (i.e. registration is not required).
     * </p>
     *
     * @param registrationRequired {@code true} if all serializable classes should be {@link #setKnownTypes(List) registered} in advance.
     */
    public void setRegistrationRequired(boolean registrationRequired) {
        this.registrationRequired = registrationRequired;
    }

    /**
     * Fluent-style version of {@link #setRegistrationRequired(boolean)}.
     *
     * @param registrationRequired {@code true} if all serializable classes should be {@link #setKnownTypes(List) registered} in advance.
     *
     * @return This instance.
     */
    public KryoCodecFactory<T> withRegistrationRequired(boolean registrationRequired) {
        setRegistrationRequired(registrationRequired);

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
     * If set to {@code true} then mapping of unknown classes (that were not explicitly registered via {@link #withKnownType(Class)})
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
