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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.esotericsoftware.kryo.util.DefaultClassResolver;
import com.esotericsoftware.kryo.util.MapReferenceResolver;
import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.codec.HekateSerializableClasses;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.Externalizable;
import java.io.IOException;
import java.lang.invoke.SerializedLambda;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import static java.util.Comparator.comparing;

class KryoCodec implements Codec<Object> {
    private static class NonResettableClassResolver extends DefaultClassResolver {
        @Override
        public void reset() {
            // Ignore reset in order to preserve cache of auto-registered classes mapping.
        }
    }

    private static final int BUFFER_SIZE = 4096; // Same with Kryo's Input/Output default buffer size.

    private static final boolean KRYO_SERIALIZERS_SUPPORTED;

    static {
        boolean supported = true;

        try {
            String className = "de.javakaffee.kryoserializers.ArraysAsListSerializer";

            Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (Throwable t) {
            supported = false;
        }

        KRYO_SERIALIZERS_SUPPORTED = supported;
    }

    @ToStringIgnore
    private final Kryo kryo;

    @ToStringIgnore
    private final Input input;

    @ToStringIgnore
    private final Output output;

    private final boolean stateful;

    public KryoCodec(KryoCodecFactory<?> factory) {
        stateful = factory.isCacheUnknownTypes();

        if (stateful) {
            kryo = statefulKryo();
        } else {
            kryo = statelessKryo();
        }

        kryo.setRegistrationRequired(factory.isRegistrationRequired());

        if (factory.getReferences() != null) {
            kryo.setReferences(factory.getReferences());
        }

        if (factory.getInstantiatorStrategy() != null) {
            kryo.setInstantiatorStrategy(factory.getInstantiatorStrategy());
        }

        // Register Hekate-internal classes
        HekateSerializableClasses.get().forEach(kryo::register);

        // Register JDK classes.
        kryo.register(Object.class);
        kryo.register(Object[].class);
        kryo.register(ArrayList.class);
        kryo.register(LinkedList.class);
        kryo.register(HashSet.class);
        kryo.register(TreeSet.class);
        kryo.register(LinkedHashSet.class);
        kryo.register(HashMap.class);
        kryo.register(TreeMap.class);
        kryo.register(LinkedHashMap.class);

        // Register custom classes.
        if (factory.getKnownTypes() != null && !factory.getKnownTypes().isEmpty()) {
            SortedSet<Class<?>> sortedTypes = new TreeSet<>(comparing(Class::getName));

            sortedTypes.addAll(factory.getKnownTypes());

            sortedTypes.forEach(kryo::register);
        }

        // Try to register extended serializers for the JDK classes that are not supported by Kryo out of the box.
        if (KRYO_SERIALIZERS_SUPPORTED) {
            JavaKaffeeSerializersRegistrar.register(kryo);
        }

        // Enforce JDK default serialization (required for writeReplace/readResolve/etc).
        kryo.addDefaultSerializer(Throwable.class, new JavaSerializer());
        kryo.addDefaultSerializer(Externalizable.class, new JavaSerializer());

        // Enable serialization of Java Lambdas.
        kryo.register(SerializedLambda.class);
        kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());

        // Register custom serializers.
        if (factory.getSerializers() != null && !factory.getSerializers().isEmpty()) {
            factory.getSerializers().forEach(kryo::register);
        }

        if (factory.getDefaultSerializers() != null && !factory.getDefaultSerializers().isEmpty()) {
            factory.getDefaultSerializers().forEach(kryo::addDefaultSerializer);
        }

        if (factory.isUnsafeIo()) {
            input = new UnsafeInput(BUFFER_SIZE);
            output = new UnsafeOutput(BUFFER_SIZE);
        } else {
            input = new Input(BUFFER_SIZE);
            output = new Output(BUFFER_SIZE);
        }
    }

    @Override
    public boolean isStateful() {
        return stateful;
    }

    @Override
    public Class<Object> baseType() {
        return Object.class;
    }

    @Override
    public Object decode(DataReader in) throws IOException {
        input.setInputStream(in.asStream());

        try {
            return kryo.readClassAndObject(input);
        } finally {
            input.setInputStream(null);
        }
    }

    @Override
    public void encode(Object obj, DataWriter out) throws IOException {
        output.setOutputStream(out.asStream());

        try {
            kryo.writeClassAndObject(output, obj);

            output.flush();
        } finally {
            output.setOutputStream(null);
        }
    }

    private Kryo statelessKryo() {
        return new Kryo(new DefaultClassResolver(), new MapReferenceResolver());
    }

    private Kryo statefulKryo() {
        return new Kryo(new NonResettableClassResolver(), new MapReferenceResolver());
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
