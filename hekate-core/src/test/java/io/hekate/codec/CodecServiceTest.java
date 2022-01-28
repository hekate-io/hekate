/*
 * Copyright 2022 The Hekate Project
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
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.internal.DefaultClusterHash;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.internal.DefaultCodecService;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.util.format.ToString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(Parameterized.class)
public class CodecServiceTest extends HekateTestBase {
    private final CodecService service;

    private final CodecFactory<Object> codecFactory;

    public CodecServiceTest(CodecFactory<Object> factory) {
        codecFactory = factory;

        service = new DefaultCodecService(codecFactory);
    }

    @Parameters(name = "{index}:{0}")
    public static Collection<CodecFactory<Object>> getCodecFactories() {
        return asList(
            new JdkCodecFactory<>(),
            new KryoCodecFactory<>(),
            new FstCodecFactory<>()
        );
    }

    @Test
    public void testEncoderDecoder() throws Exception {
        encodeDecode(String.class, "some string", Assert::assertEquals);
        encodeDecode(ClusterNodeId.class, newNodeId(), Assert::assertEquals);
        encodeDecode(ClusterAddress.class, newAddress(1), Assert::assertEquals);
        encodeDecode(ClusterNode.class, newNode(), Assert::assertEquals);
        encodeDecode(Set.class, singleton("one"), Assert::assertEquals);
        encodeDecode(List.class, singletonList("one"), Assert::assertEquals);
        encodeDecode(Map.class, singletonMap("one", "one"), Assert::assertEquals);
        encodeDecode(List.class, asList("one", "two", "three"), Assert::assertEquals);
        encodeDecode(ClusterTopology.class, DefaultClusterTopology.of(1, toSet(newNode(), newNode(), newNode())), Assert::assertEquals);
        encodeDecode(ClusterHash.class, new DefaultClusterHash(asList(newNode(), newNode(), newNode())), Assert::assertEquals);
    }

    @Test
    public void testEncodeDecode() throws Exception {
        assertEquals("test", service.decode(service.encode("test")));
    }

    @Test
    public void testForFactory() throws IOException {
        SingletonCodecFactory<String> factory = new SingletonCodecFactory<>(new Codec<String>() {
            @Override
            public boolean isStateful() {
                return false;
            }

            @Override
            public Class<String> baseType() {
                return String.class;
            }

            @Override
            public String decode(DataReader in) throws IOException {
                return in.readUTF();
            }

            @Override
            public void encode(String obj, DataWriter out) throws IOException {
                out.writeUTF(obj);
            }
        });

        encodeDecode(service.forFactory(factory), "some string", Assert::assertEquals);
    }

    @Test
    public void testEncodeDecodeFunctions() throws Exception {
        byte[] bytes = service.encode("test", (obj, out) -> out.writeUTF(obj));

        assertEquals("test", service.decode(bytes, DataInput::readUTF));

        byte[] larger = new byte[bytes.length + 6];

        System.arraycopy(bytes, 0, larger, 3, bytes.length);

        assertEquals("test", service.decode(larger, 3, bytes.length, DataInput::readUTF));
    }

    @Test
    public void testEncoderDecoderWithCustomFunctions() throws Exception {
        EncoderDecoder<String> codec = service.forType(
            String.class,
            (str, out) -> out.writeUTF(str),
            DataInput::readUTF
        );

        assertEquals("test", codec.decode(codec.encode("test")));
    }

    @Test
    public void testCodecFactory() {
        assertSame(codecFactory, ThreadLocalCodecFactory.tryUnwrap(service.codecFactory()));
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(CodecService.class, service), service.toString());
    }

    private <T> void encodeDecode(Class<T> type, T before, BiConsumer<T, T> check) throws IOException {
        EncoderDecoder<T> codec = service.forType(type);

        encodeDecode(codec, before, check);
    }

    private <T> void encodeDecode(EncoderDecoder<T> codec, T before, BiConsumer<T, T> check) throws IOException {
        encodeDecodeAsStream(codec, before, check);

        encodeDecodeAsByteArray(codec, before, check);

        encodeDecodeAsByteArrayWithOffset(codec, before, check);
    }

    private <T> void encodeDecodeAsStream(EncoderDecoder<T> codec, T before, BiConsumer<T, T> check) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        codec.encode(before, buf);

        T after = codec.decode(new ByteArrayInputStream(buf.toByteArray()));

        check.accept(before, after);
    }

    private <T> void encodeDecodeAsByteArray(EncoderDecoder<T> codec, T before, BiConsumer<T, T> check) throws IOException {
        check.accept(before, codec.decode(codec.encode(before)));
    }

    private <T> void encodeDecodeAsByteArrayWithOffset(EncoderDecoder<T> codec, T before, BiConsumer<T, T> check) throws IOException {
        byte[] bytes = codec.encode(before);

        byte[] bytesWithOffset = new byte[bytes.length + 6];

        System.arraycopy(bytes, 0, bytesWithOffset, 3, bytes.length);

        T v = codec.decode(bytesWithOffset, 3, bytes.length);

        check.accept(before, v);
    }
}
