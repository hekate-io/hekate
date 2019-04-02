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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.hekate.HekateTestBase;
import io.hekate.codec.CodecTestBase.ObjA;
import io.hekate.codec.CodecTestBase.ObjB;
import java.util.Collections;
import org.junit.Test;
import org.objenesis.strategy.InstantiatorStrategy;
import org.objenesis.strategy.StdInstantiatorStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class KryoCodecFactoryTest extends HekateTestBase {
    private final KryoCodecFactory<Object> factory = new KryoCodecFactory<>();

    @Test
    public void testKnownTypes() {
        assertNull(factory.getKnownTypes());

        factory.setKnownTypes(Collections.singletonList(ObjA.class));

        assertEquals(ObjA.class, factory.getKnownTypes().get(0));

        factory.setKnownTypes(null);

        assertNull(factory.getKnownTypes());

        assertSame(factory, factory.withKnownType(ObjB.class));

        assertEquals(ObjB.class, factory.getKnownTypes().get(0));

        factory.getKnownTypes().remove(0);

        assertFalse(factory.getKnownTypes().contains(ObjB.class));

        assertSame(factory, factory.withKnownTypes(Collections.singletonList(ObjA.class)));

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testRegistrationRequired() {
        assertFalse(factory.isRegistrationRequired());

        factory.setRegistrationRequired(true);

        assertTrue(factory.isRegistrationRequired());

        assertSame(factory, factory.withRegistrationRequired(false));

        assertFalse(factory.isRegistrationRequired());
    }

    @Test
    public void testSerializers() {
        assertNull(factory.getSerializers());

        Serializer<Object> serializer = new Serializer<Object>() {
            @Override
            public void write(Kryo kryo, Output output, Object object) {
                // No-op.
            }

            @Override
            public Object read(Kryo kryo, Input input, Class<Object> type) {
                return null;
            }
        };

        factory.setSerializers(Collections.singletonMap(ObjA.class, serializer));

        assertSame(serializer, factory.getSerializers().get(ObjA.class));

        factory.setSerializers(null);

        assertNull(factory.getSerializers());

        assertSame(factory, factory.withSerializer(Object.class, serializer));

        assertSame(serializer, factory.getSerializers().get(Object.class));

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testDefaultSerializer() {
        assertNull(factory.getDefaultSerializers());

        Serializer<Object> serializer = new Serializer<Object>() {
            @Override
            public void write(Kryo kryo, Output output, Object object) {
                // No-op.
            }

            @Override
            public Object read(Kryo kryo, Input input, Class<Object> type) {
                return null;
            }
        };

        factory.setDefaultSerializers(Collections.singletonMap(ObjA.class, serializer));

        assertSame(serializer, factory.getDefaultSerializers().get(ObjA.class));

        factory.setDefaultSerializers(null);

        assertNull(factory.getDefaultSerializers());

        assertSame(factory, factory.withDefaultSerializer(Object.class, serializer));

        assertSame(serializer, factory.getDefaultSerializers().get(Object.class));

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testInstantiatorStrategy() throws Exception {
        assertNotNull(factory.getInstantiatorStrategy());

        InstantiatorStrategy strategy = new StdInstantiatorStrategy();

        factory.setInstantiatorStrategy(strategy);

        assertSame(strategy, factory.getInstantiatorStrategy());

        factory.setInstantiatorStrategy(null);

        assertNotNull(factory.createCodec());

        assertNull(factory.getInstantiatorStrategy());

        assertSame(factory, factory.withInstantiatorStrategy(strategy));

        assertSame(strategy, factory.getInstantiatorStrategy());

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testUnsafeIo() throws Exception {
        assertTrue(factory.isUnsafeIo());

        factory.setUnsafeIo(false);

        assertFalse(factory.isUnsafeIo());

        assertSame(factory, factory.withUnsafeIo(true));

        assertTrue(factory.isUnsafeIo());

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testReferences() throws Exception {
        assertNull(factory.getReferences());

        factory.setReferences(false);

        assertFalse(factory.getReferences());

        assertSame(factory, factory.withReferences(true));

        assertTrue(factory.getReferences());

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testCacheUnknownTypes() throws Exception {
        assertFalse(factory.isCacheUnknownTypes());

        factory.setCacheUnknownTypes(true);

        assertTrue(factory.isCacheUnknownTypes());

        assertSame(factory, factory.withCacheUnknownTypes(false));

        assertFalse(factory.isCacheUnknownTypes());

        assertNotNull(factory.createCodec());
    }
}
