/*
 * Copyright 2018 The Hekate Project
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
import io.hekate.cluster.internal.DefaultClusterHash;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.internal.DefaultCodecService;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.util.format.ToString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class CodecServiceTest extends HekateTestBase {
    private final CodecService service;

    public CodecServiceTest(CodecFactory<Object> factory) {
        service = new DefaultCodecService(factory);
    }

    @Parameters(name = "{index}:{0}")
    public static Collection<CodecFactory<Object>> getCodecFactories() {
        return Arrays.asList(
            new JdkCodecFactory<>(),
            new KryoCodecFactory<>(),
            new FstCodecFactory<>()
        );
    }

    @Test
    public void test() throws Exception {
        encodeDecode("some string", Assert::assertEquals);
        encodeDecode(newNodeId(), Assert::assertEquals);
        encodeDecode(newAddress(1), Assert::assertEquals);
        encodeDecode(newNode(), Assert::assertEquals);
        encodeDecode(Collections.singleton("one"), Assert::assertEquals);
        encodeDecode(Collections.singletonList("one"), Assert::assertEquals);
        encodeDecode(Collections.singletonMap("one", "one"), Assert::assertEquals);
        encodeDecode(Arrays.asList("one", "two", "three"), Assert::assertEquals);
        encodeDecode(DefaultClusterTopology.of(1, toSet(newNode(), newNode(), newNode())), Assert::assertEquals);
        encodeDecode(new DefaultClusterHash(Arrays.asList(newNode(), newNode(), newNode())), Assert::assertEquals);
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(CodecService.class, service), service.toString());
    }

    private <T> void encodeDecode(T before, BiConsumer<T, T> check) throws IOException {
        T after = service.decodeFromByteArray(service.encodeToByteArray(before));

        check.accept(before, after);

        ByteArrayOutputStream buf = new ByteArrayOutputStream();

        service.encodeToStream(before, buf);

        after = service.decodeFromStream(new ByteArrayInputStream(buf.toByteArray()));

        check.accept(before, after);
    }
}
