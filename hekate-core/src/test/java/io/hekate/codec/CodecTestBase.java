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

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

public abstract class CodecTestBase<T extends CodecFactory<Object>> extends HekateTestBase {
    public static class ObjA implements Serializable {
        private static final long serialVersionUID = 1;

        private int intVal;

        private String strVal;

        public ObjA() {
            // No-op.
        }

        public ObjA(int intVal, String strVal) {
            this.intVal = intVal;
            this.strVal = strVal;
        }

        public int getIntVal() {
            return intVal;
        }

        public String getStrVal() {
            return strVal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ObjA)) {
                return false;
            }

            ObjA objA = (ObjA)o;

            if (intVal != objA.intVal) {
                return false;
            }

            return strVal != null ? strVal.equals(objA.strVal) : objA.strVal == null;
        }

        @Override
        public int hashCode() {
            int result = intVal;

            result = 31 * result + (strVal != null ? strVal.hashCode() : 0);

            return result;
        }
    }

    public static class ObjB implements Serializable {
        private static final long serialVersionUID = 1;

        private ObjA objVal;

        private List<ObjA> listVal;

        public ObjB() {
            // No-op.
        }

        public ObjB(ObjA objVal, List<ObjA> listVal) {
            this.objVal = objVal;
            this.listVal = listVal;
        }

        public ObjA getObjVal() {
            return objVal;
        }

        public List<ObjA> getListVal() {
            return listVal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ObjB)) {
                return false;
            }

            ObjB objB = (ObjB)o;

            if (objVal != null ? !objVal.equals(objB.objVal) : objB.objVal != null) {
                return false;
            }

            return listVal != null ? listVal.equals(objB.listVal) : objB.listVal == null;
        }

        @Override
        public int hashCode() {
            int result = objVal != null ? objVal.hashCode() : 0;

            result = 31 * result + (listVal != null ? listVal.hashCode() : 0);

            return result;
        }
    }

    protected final T factory;

    public CodecTestBase(T factory) {
        this.factory = factory;
    }

    public static List<Class<?>> getKnownTestTypes() {
        List<Class<?>> types = new ArrayList<>();

        types.add(ObjA.class);
        types.add(ObjB.class);

        return types;
    }

    @Test
    public void testUnmodifiableMap() throws Exception {
        Map<Object, Object> map = new HashMap<>();

        map.put(new ObjA(), new ObjA());
        map.put(new ObjB(), new ObjB());

        Codec<Object> codec = factory.createCodec();

        Map<Object, Object> decoded = encodeDecode(codec, codec, Collections.unmodifiableMap(map));

        assertEquals(map, decoded);
    }

    @Test
    public void testUnmodifiableSet() throws Exception {
        Set<Object> set = new HashSet<>();

        set.add(new ObjA());
        set.add(new ObjB());

        Codec<Object> codec = factory.createCodec();

        Set<Object> decoded = encodeDecode(codec, codec, Collections.unmodifiableSet(set));

        assertEquals(set, decoded);
    }

    @Test
    public void testUnmodifiableList() throws Exception {
        List<Object> list = new ArrayList<>();

        list.add(new ObjA());
        list.add(new ObjB());

        Codec<Object> codec = factory.createCodec();

        List<Object> decoded = encodeDecode(codec, codec, Collections.unmodifiableList(list));

        assertEquals(list, decoded);
    }

    @Test
    public void testArrayAsList() throws Exception {
        List<Object> list = Arrays.asList(new ObjA(), new ObjB());

        Codec<Object> codec = factory.createCodec();

        List<Object> decoded = encodeDecode(codec, codec, list);

        assertEquals(list, decoded);
    }

    @Test
    public void testToString() {
        assertTrue(factory.toString().startsWith(factory.getClass().getSimpleName()));

        Codec<Object> codec = factory.createCodec();

        assertEquals(ToString.format(codec), codec.toString());
    }

    @Test
    public void testBaseType() {
        assertEquals(Object.class, factory.createCodec().baseType());
    }

    @Test
    public void testEncodeDecode() throws Exception {
        Codec<Object> encoder = factory.createCodec();
        Codec<Object> decoder = factory.createCodec();

        repeat(3, i -> {
            ObjA objA1 = new ObjA(1, new String(new byte[1024 * 256], UTF_8));
            ObjA objA2 = new ObjA(2, "test2");
            ObjA objA3 = new ObjA(3, "test3");

            List<ObjA> list = new ArrayList<>();

            list.add(objA2);
            list.add(objA3);

            ObjB objB = new ObjB(objA1, list);

            ObjA objA1Decoded = encodeDecode(encoder, decoder, objA1);

            assertNotSame(objA1, objA1Decoded);
            assertEquals(objA1.getIntVal(), objA1Decoded.getIntVal());
            assertEquals(objA1.getStrVal(), objA1Decoded.getStrVal());

            ObjB objBDecoded = encodeDecode(encoder, decoder, objB);

            assertNotSame(objB, objBDecoded);
            assertEquals(objB.getObjVal().getIntVal(), objBDecoded.getObjVal().getIntVal());
            assertEquals(objB.getObjVal().getStrVal(), objBDecoded.getObjVal().getStrVal());

            assertEquals(objB.getListVal().size(), objB.getListVal().size());

            assertEquals(objB.getListVal().get(0).getIntVal(), objBDecoded.getListVal().get(0).getIntVal());
            assertEquals(objB.getListVal().get(0).getStrVal(), objBDecoded.getListVal().get(0).getStrVal());

            assertEquals(objB.getListVal().get(1).getIntVal(), objBDecoded.getListVal().get(1).getIntVal());
            assertEquals(objB.getListVal().get(1).getStrVal(), objBDecoded.getListVal().get(1).getStrVal());
        });
    }

    @SuppressWarnings("unchecked")
    protected <V> V encodeDecode(Codec<Object> encoder, Codec<Object> decoder, V msg) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        StreamDataWriter out = new StreamDataWriter(bout);

        encoder.encode(msg, out);

        say("Encoded size (bytes): " + bout.size());

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

        StreamDataReader in = new StreamDataReader(bin);

        return (V)decoder.decode(in);
    }
}
