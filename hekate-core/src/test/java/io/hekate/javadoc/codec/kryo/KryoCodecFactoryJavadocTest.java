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

package io.hekate.javadoc.codec.kryo;

import io.hekate.HekateTestBase;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import org.junit.Test;

public class KryoCodecFactoryJavadocTest extends HekateTestBase {
    public static class MyClass1 {
        // No-op.
    }

    public static class MyClass2 {
        // No-op.
    }

    @Test
    public void test() throws Exception {
        // Start:configuration
        // Prepare Kryo codec factory.
        KryoCodecFactory<Object> kryoCodec = new KryoCodecFactory<>()
            .withReferences(true)
            .withUnsafeIo(true)
            // Register known types.
            .withKnownType(MyClass1.class)
            .withKnownType(MyClass2.class);

        // Register Kryo codec factory and start a new node.
        Hekate node = new HekateBootstrap()
            .withDefaultCodec(kryoCodec)
            // ...other options...
            .join();
        // End:configuration

        node.leave();
    }
}
