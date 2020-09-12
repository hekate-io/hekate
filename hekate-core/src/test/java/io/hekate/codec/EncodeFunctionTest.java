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

package io.hekate.codec;

import io.hekate.HekateTestBase;
import java.io.IOException;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class EncodeFunctionTest extends HekateTestBase {
    @Test
    public void testEncodeUncheckedWithIoException() {
        EncodeFunction<Object> encode = (obj, out) -> {
            throw new IOException("TEST");
        };

        expect(CodecException.class, () -> encode.encodeUnchecked(new Object(), mock(DataWriter.class)));
        expect(CodecException.class, () -> encode.encodeUnchecked(null, mock(DataWriter.class)));
    }

    @Test
    public void testEncodeUncheckedWithRuntimeException() {
        EncodeFunction<Object> encode = (obj, out) -> {
            throw new RuntimeException("TEST");
        };

        expect(RuntimeException.class, () -> encode.encodeUnchecked(new Object(), mock(DataWriter.class)));
        expect(RuntimeException.class, () -> encode.encodeUnchecked(null, mock(DataWriter.class)));
    }
}
