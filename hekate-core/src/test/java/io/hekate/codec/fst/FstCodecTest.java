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

package io.hekate.codec.fst;

import io.hekate.codec.CodecTestBase;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertFalse;

@RunWith(Parameterized.class)
public class FstCodecTest extends CodecTestBase<FstCodecFactory<Object>> {
    public FstCodecTest(FstCodecFactory<Object> factory) {
        super(factory);
    }

    @Parameters(name = "{index}: factory={0}")
    public static Collection<Object[]> getParams() {
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[]{new FstCodecFactory<>()});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(false)});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(true)});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(false).withKnownTypes(getKnownTestTypes())});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(true).withKnownTypes(getKnownTestTypes())});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(true).withSharedReferences(true)});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(true).withSharedReferences(false)});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(false).withKnownTypes(getKnownTestTypes())});
        params.add(new Object[]{new FstCodecFactory<>().withUseUnsafe(true).withKnownTypes(getKnownTestTypes())});

        return params;
    }

    @Test
    public void testStateless() {
        assertFalse(factory.createCodec().isStateful());
    }
}
