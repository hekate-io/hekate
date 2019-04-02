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

package io.hekate.codec.fst;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.HekateSerializableClasses;
import io.hekate.core.HekateBootstrap;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.simpleapi.DefaultCoder;
import org.nustaq.serialization.simpleapi.FSTCoder;
import org.nustaq.serialization.simpleapi.OnHeapCoder;

import static java.util.Comparator.comparing;

/**
 * <span class="startHere">&laquo; start here</span><a href="http://ruedigermoeller.github.io/fast-serialization/" target="_blank">FST</a>
 * -based implementation of {@link CodecFactory} interface.
 *
 * <h2>Module dependency</h2>
 * <p>
 * FST integration is provided by the 'hekate-codec-fst' module and can be imported into the project dependency management system as in the
 * example below:
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
 *   <artifactId>hekate-codec-fst</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-codec-fst', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-codec-fst" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
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

    private List<Class<?>> knownTypes;

    @Override
    public Codec<T> createCodec() {
        FSTConfiguration fst;

        if (useUnsafe) {
            fst = FSTConfiguration.createUnsafeBinaryConfiguration();
        } else {
            fst = FSTConfiguration.createDefaultConfiguration();
        }

        // Register Hekate-internal classes
        HekateSerializableClasses.get().forEach(fst::registerClass);

        // Register custom classes
        if (knownTypes != null && !knownTypes.isEmpty()) {
            SortedSet<Class<?>> sortedTypes = new TreeSet<>(comparing(Class::getName));

            sortedTypes.addAll(knownTypes);

            sortedTypes.forEach(fst::registerClass);
        }

        if (sharedReferences != null) {
            fst.setShareReferences(sharedReferences);
        }

        return newCodec(fst);
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
     * Returns the list of known Java types (see {@link #setKnownTypes(List)}).
     *
     * @return List of known Java types.
     */
    public List<Class<?>> getKnownTypes() {
        return knownTypes;
    }

    /**
     * Sets the list of known Java types.
     *
     * <p>
     * <b>Notice:</b>
     * Exactly the same list of types should be registered on all cluster nodes. Otherwise FST will not be able deserialize data.
     * Fore more details please see {@link FSTConfiguration#registerClass(Class[])}.
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
    public FstCodecFactory<T> withKnownType(Class<?> type) {
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
    public FstCodecFactory<T> withKnownTypes(List<Class<?>> types) {
        if (knownTypes == null) {
            knownTypes = new ArrayList<>();
        }

        knownTypes.addAll(types);

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
