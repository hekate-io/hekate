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

package io.hekate.network.netty;

import io.hekate.HekateTestBase;
import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInput;
import java.io.IOException;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NettyMessageTest extends HekateTestBase {
    @Test
    public void testPreview() throws Exception {
        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(10);
        buf.writeBoolean(true);
        buf.writeInt(100);
        buf.writeLong(1000);
        buf.writeDouble(1.01);

        NettyMessage msg = new NettyMessage(buf, new Codec<Object>() {
            @Override
            public boolean isStateful() {
                return false;
            }

            @Override
            public Class<Object> baseType() {
                return Object.class;
            }

            @Override
            public Object decode(DataReader in) throws IOException {
                throw new AssertionError("Should not be called.");
            }

            @Override
            public void encode(Object obj, DataWriter out) throws IOException {
                throw new AssertionError("Should not be called.");
            }
        });

        assertEquals((byte)10, (byte)msg.preview(DataInput::readByte));
        assertEquals(0, buf.readerIndex());

        assertTrue(msg.previewBoolean(m -> {
            m.skipBytes(1);

            return m.readBoolean();
        }));
        assertEquals(0, buf.readerIndex());

        assertEquals(100, msg.previewInt(m -> {
            m.skipBytes(2);

            return m.readInt();
        }));
        assertEquals(0, buf.readerIndex());

        assertEquals(1000, msg.previewLong(m -> {
            m.skipBytes(6);

            return m.readLong();
        }));
        assertEquals(0, buf.readerIndex());

        assertEquals(1.01, msg.previewDouble(m -> {
            m.skipBytes(14);

            return m.readDouble();
        }), 1000);
        assertEquals(0, buf.readerIndex());

        assertEquals(1, buf.refCnt());

        msg.release();

        assertEquals(0, buf.refCnt());
    }

    @Test
    public void testHandleAsync() throws Exception {
        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(1);

        NettyMessage msg = new NettyMessage(buf, new Codec<Object>() {
            @Override
            public boolean isStateful() {
                return false;
            }

            @Override
            public Class<Object> baseType() {
                return Object.class;
            }

            @Override
            public Object decode(DataReader in) throws IOException {
                return in.readByte();
            }

            @Override
            public void encode(Object obj, DataWriter out) throws IOException {
                throw new AssertionError("Should not be called.");
            }
        });

        Object val;

        ExecutorService thread = Executors.newSingleThreadExecutor();

        try {
            Exchanger<Object> ref = new Exchanger<>();

            msg.handleAsync(thread, m -> {
                try {
                    ref.exchange(m.decode());
                } catch (InterruptedException | IOException e) {
                    // No-op.
                }
            });

            val = ref.exchange(null);
        } finally {
            thread.shutdownNow();

            thread.awaitTermination(AWAIT_TIMEOUT, TimeUnit.SECONDS);
        }

        assertEquals((byte)1, val);
        assertEquals(1, buf.refCnt());
    }
}
