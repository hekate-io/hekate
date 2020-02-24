/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.messaging;

import io.hekate.HekateTestBase;
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.messaging.MessageMetaData.Key;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessageMetaDataTest extends HekateTestBase {
    @Test
    public void testKey() {
        Key<byte[]> key = Key.of("test", MessageMetaData.MetaDataCodec.BYTES);

        assertEquals("test", key.name());
        assertEquals("test", key.toString());
    }

    @Test
    public void testEmpty() throws IOException {
        MessageMetaData m = new MessageMetaData();

        assertTrue(m.isEmpty());
        assertEquals(0, m.size());
        assertEquals(MessageMetaData.class.getSimpleName() + "[size=0]", m.toString());

        assertNull(m.get(Key.of("test1", MessageMetaData.MetaDataCodec.BYTES)));

        MessageMetaData m2 = encodeDecode(m);

        assertTrue(m2.isEmpty());
        assertEquals(0, m2.size());
    }

    @Test
    public void testNotEmpty() throws IOException {
        MessageMetaData m = new MessageMetaData();

        for (int i = 0; i < 100; i++) {
            Key<byte[]> key1 = Key.of("bytes" + i, MessageMetaData.MetaDataCodec.BYTES);
            Key<String> key2 = Key.of("text" + i, MessageMetaData.MetaDataCodec.TEXT);

            // Put new values.
            byte[] bytes = randomBytes();
            String string = UUID.randomUUID().toString();

            m.set(key1, bytes);
            m.set(key2, string);

            assertFalse(m.isEmpty());
            assertEquals((i + 1) * 2, m.size());
            assertArrayEquals(bytes, m.get(key1));
            assertEquals(string, m.get(key2));

            // Update existing values.
            bytes = randomBytes();
            string = UUID.randomUUID().toString();

            m.set(key1, bytes);
            m.set(key2, string);

            assertEquals((i + 1) * 2, m.size());
            assertArrayEquals(bytes, m.get(key1));
            assertEquals(string, m.get(key2));

            MessageMetaData m2 = encodeDecode(m);

            assertEquals(m.size(), m2.size());
            assertArrayEquals(bytes, m.get(key1));
            assertEquals(string, m.get(key2));
        }
    }

    private MessageMetaData encodeDecode(MessageMetaData metaData) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        metaData.writeTo(new StreamDataWriter(bout));

        return MessageMetaData.readFrom(new StreamDataReader(new ByteArrayInputStream(bout.toByteArray())));
    }

    private byte[] randomBytes() {
        byte[] bytes = new byte[100];

        ThreadLocalRandom.current().nextBytes(bytes);

        return bytes;
    }
}
