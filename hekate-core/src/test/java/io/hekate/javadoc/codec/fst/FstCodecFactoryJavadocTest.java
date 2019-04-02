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

package io.hekate.javadoc.codec.fst;

import io.hekate.HekateTestBase;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import java.io.Serializable;
import org.junit.Test;

public class FstCodecFactoryJavadocTest extends HekateTestBase {
    public static class MyClass1 implements Serializable {
        private static final long serialVersionUID = 1;
    }

    public static class MyClass2 implements Serializable {
        private static final long serialVersionUID = 1;
    }

    @Test
    public void test() throws Exception {
        // Start:configuration
        // Prepare FST codec factory.
        FstCodecFactory<Object> fstCodec = new FstCodecFactory<>()
            .withUseUnsafe(true)
            // Register known types.
            .withKnownType(MyClass1.class)
            .withKnownType(MyClass2.class);

        // Register FST codec factory and start a new node.
        Hekate node = new HekateBootstrap()
            .withDefaultCodec(fstCodec)
            // ...other options...
            .join();
        // End:configuration

        node.leave();
    }
}
