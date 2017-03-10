/*
 * Copyright 2017 The Hekate Project
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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

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
    }

    protected final T factory;

    public CodecTestBase(T factory) {
        this.factory = factory;
    }

    public static Map<Integer, Class<?>> getKnownTestTypes() {
        Map<Integer, Class<?>> types = new HashMap<>();

        types.put(types.size() + 1, ObjA.class);
        types.put(types.size() + 1, ObjB.class);

        return types;
    }

    @Test
    public void testToString() {
        assertTrue(factory.toString().startsWith(factory.getClass().getSimpleName()));

        Codec<Object> codec = factory.createCodec();

        assertTrue(codec.toString().startsWith(codec.getClass().getSimpleName()));
    }

    @Test
    public void testEncodeDecode() throws Exception {
        Codec<Object> encoder = factory.createCodec();
        Codec<Object> decoder = factory.createCodec();

        repeat(3, i -> {
            ObjA objA1 = new ObjA(1, new String(new byte[1024 * 256]));
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
    protected <V> V encodeDecode(Codec<Object> encoder, Codec<Object> decoder, Object msg) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        StreamDataWriter out = new StreamDataWriter(bout);

        encoder.encode(msg, out);

        say("Encoded size (bytes): " + bout.size());

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

        StreamDataReader in = new StreamDataReader(bin);

        return (V)decoder.decode(in);
    }
}
