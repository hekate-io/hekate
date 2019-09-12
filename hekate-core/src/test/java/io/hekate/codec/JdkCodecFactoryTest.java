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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class JdkCodecFactoryTest extends HekateTestBase {
    public static class DummyMessage implements Serializable {
        private static final long serialVersionUID = 1;

        private final int intVal;

        private final String stringVal;

        public DummyMessage(int intVal, String stringVal) {
            this.intVal = intVal;
            this.stringVal = stringVal;
        }

        public int getIntVal() {
            return intVal;
        }

        public String getStringVal() {
            return stringVal;
        }
    }

    @Test
    public void testEncodeDecode() throws Exception {
        repeat(5, i -> {
            DummyMessage before = new DummyMessage(i, "str_" + i);

            DummyMessage after = encodeDecode(before);

            assertNotNull(after);
            assertEquals(before.getIntVal(), after.getIntVal());
            assertEquals(before.getStringVal(), after.getStringVal());
        });
    }

    private DummyMessage encodeDecode(DummyMessage msg) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        Codec<DummyMessage> encoder = new JdkCodecFactory<DummyMessage>().createCodec();

        encoder.encode(msg, new StreamDataWriter(bout));

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

        Codec<DummyMessage> decoder = new JdkCodecFactory<DummyMessage>().createCodec();

        return decoder.decode(new StreamDataReader(bin));
    }
}
