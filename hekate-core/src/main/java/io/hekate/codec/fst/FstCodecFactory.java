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

package io.hekate.codec.fst;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.util.format.ToString;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;
import org.nustaq.serialization.simpleapi.OnHeapCoder;

/**
 * <span class="startHere">&laquo; start here</span><a href="http://ruedigermoeller.github.io/fast-serialization/" target="_blank">FST</a>
 * -based implementation of {@link CodecFactory} interface.
 *
 * <h2>Module dependency</h2>
 * <p>
 * FST integration requires
 * <a href="https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22de.ruedigermoeller%22%20a%3A%22fst%22" target="_blank">
 * 'de.ruedigermoeller:fst'
 * </a>
 * to be on the project's classpath.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * Each call of {@link #createCodec()} method will produces a new independent {@link FSTCoder} instance and will configure it
 * according to properties of this factory (see description of property setters).
 * ${source: codec/fst/FstCodecFactoryJavadocTest.java#configuration}
 * </p>
 *
 * @param <T> Base type of data that should be supported by this factory.
 *
 * @see HekateBootstrap#setDefaultCodec(CodecFactory)
 */
public class FstCodecFactory<T> implements CodecFactory<T> {
    private boolean useUnsafe = true;

    private Boolean sharedReferences;

    private Map<Integer, Class<?>> knownTypes;

    @Override
    public Codec<T> createCodec() {
        Class<?>[] preRegister;

        if (knownTypes == null || knownTypes.isEmpty()) {
            preRegister = null;
        } else {
            preRegister = new TreeMap<>(knownTypes).values().toArray(new Class[knownTypes.size()]);
        }

        FSTConfiguration cfg;

        if (useUnsafe) {
            cfg = FSTConfiguration.createUnsafeBinaryConfiguration();
        } else {
            cfg = FSTConfiguration.createDefaultConfiguration();
        }

        if (preRegister != null) {
            for (Class<?> type : preRegister) {
                cfg.registerClass(type);
            }
        }

        if (sharedReferences != null) {
            cfg.setShareReferences(sharedReferences);
        }

        return newCodec(cfg);
    }

    /**
     * Returns the flag that indicates which implementation of {@link FSTCoder} should be used (see {@link #setUseUnsafe(boolean)}).
     *
     * @return Flag that indicates which implementation of {@link FSTCoder} should be used.
     */
    public boolean isUseUnsafe() {
        return useUnsafe;
    }

    /**
     * Sets the flag that indicates which implementation of {@link FSTCoder} should be used.
     *
     * <p>
     * If {@code true} then {@link OnHeapCoder} will be used. If {@code false} then {@link DefaultCoder} will be used.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code true}.
     * </p>
     *
     * @param useUnsafe Flag that indicates which implementation of {@link FSTCoder} should be used.
     */
    public void setUseUnsafe(boolean useUnsafe) {
        this.useUnsafe = useUnsafe;
    }

    /**
     * Fluent-style version of {@link #setUseUnsafe(boolean)}.
     *
     * @param useUnsafe Flag that indicates which implementation of {@link FSTCoder} should be used.
     *
     * @return This instance.
     */
    public FstCodecFactory<T> withUseUnsafe(boolean useUnsafe) {
        setUseUnsafe(useUnsafe);

        return this;
    }

    /**
     * Returns the value that should override the {@link FSTConfiguration#setShareReferences(boolean)} flag (see {@link
     * #setSharedReferences(Boolean)}).
     *
     * @return Value that should override the {@link FSTConfiguration#setShareReferences(boolean)} flag.
     */
    public Boolean getSharedReferences() {
        return sharedReferences;
    }

    /**
     * Sets the value that should override the {@link FSTConfiguration#setShareReferences(boolean)} flag.
     *
     * @param sharedReferences Value that should override the {@link FSTConfiguration#setShareReferences(boolean)} flag.
     */
    public void setSharedReferences(Boolean sharedReferences) {
        this.sharedReferences = sharedReferences;
    }

    /**
     * Fluent-style version of {@link #setSharedReferences(Boolean)}.
     *
     * @param sharedReferences Value that should override the {@link FSTConfiguration#setShareReferences(boolean)} flag.
     *
     * @return This instance.
     */
    public FstCodecFactory<T> withSharedReferences(Boolean sharedReferences) {
        setSharedReferences(sharedReferences);

        return this;
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
     * Sets the map of known java types and their IDs. Such types will be registered via {@link FSTConfiguration#registerClass(Class[])}
     * in the ascending order of their IDs.
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
    public FstCodecFactory<T> withKnownType(int id, Class<?> type) {
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
    public FstCodecFactory<T> withKnownTypes(Map<Integer, Class<?>> types) {
        if (knownTypes == null) {
            knownTypes = new HashMap<>();
        }

        knownTypes.putAll(types);

        return this;
    }

    @SuppressWarnings("unchecked")
    private Codec<T> newCodec(FSTConfiguration cfg) {
        return (Codec<T>)new FstCodec(cfg);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
