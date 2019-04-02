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

package io.hekate;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import java.util.Collection;
import java.util.stream.Stream;
import org.junit.runners.Parameterized.Parameters;

public abstract class HekateNodeMultiCodecTestBase extends HekateNodeParamTestBase {
    public static class MultiCodecTestContext extends HekateTestContext {
        private final CodecFactory<Object> codecFactory;

        public MultiCodecTestContext(HekateTestContext src, CodecFactory<Object> codecFactory) {
            super(src);

            this.codecFactory = codecFactory;
        }

        public CodecFactory<Object> codecFactory() {
            return codecFactory;
        }
    }

    private final MultiCodecTestContext ctx;

    public HekateNodeMultiCodecTestBase(MultiCodecTestContext ctx) {
        super(ctx);

        this.ctx = ctx;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<MultiCodecTestContext> getLockAsyncTestContexts() {
        return mapTestContext(p -> Stream.of(
            new MultiCodecTestContext(p, new KryoCodecFactory<>()),
            new MultiCodecTestContext(p, new FstCodecFactory<>()),
            new MultiCodecTestContext(p, new JdkCodecFactory<>())
        ));
    }

    @Override
    protected CodecFactory<Object> defaultCodec() {
        return ctx.codecFactory();
    }
}
