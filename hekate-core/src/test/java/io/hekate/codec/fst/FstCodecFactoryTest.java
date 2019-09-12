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

import io.hekate.HekateTestBase;
import io.hekate.codec.CodecTestBase.ObjA;
import io.hekate.codec.CodecTestBase.ObjB;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FstCodecFactoryTest extends HekateTestBase {
    private final FstCodecFactory<Object> factory = new FstCodecFactory<>();

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
    public void testUseUnsafe() throws Exception {
        assertTrue(factory.isUseUnsafe());

        factory.setUseUnsafe(false);

        assertFalse(factory.isUseUnsafe());

        assertSame(factory, factory.withUseUnsafe(true));

        assertTrue(factory.isUseUnsafe());

        assertNotNull(factory.createCodec());
    }

    @Test
    public void testReferences() throws Exception {
        assertNull(factory.getSharedReferences());

        factory.setSharedReferences(false);

        assertFalse(factory.getSharedReferences());

        assertSame(factory, factory.withSharedReferences(true));

        assertTrue(factory.getSharedReferences());

        assertNotNull(factory.createCodec());
    }
}
