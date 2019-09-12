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
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ThreadLocalCodecFactoryTest extends HekateTestBase {
    private Codec<Object> codecMock;

    private CodecFactory<Object> factoryMock;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        codecMock = mock(Codec.class);
        when(codecMock.isStateful()).thenReturn(false);

        factoryMock = mock(CodecFactory.class);
        when(factoryMock.createCodec()).thenReturn(codecMock);
    }

    @Test
    public void testCodec() throws IOException {
        CodecFactory<Object> threadLocal = ThreadLocalCodecFactory.tryWrap(factoryMock);

        verify(factoryMock).createCodec();
        verify(codecMock).isStateful();
        verify(codecMock).baseType();
        verifyNoMoreInteractions(codecMock, factoryMock);
        clearInvocations(factoryMock, codecMock);

        assertTrue(threadLocal instanceof ThreadLocalCodecFactory);

        Codec<Object> codec = threadLocal.createCodec();

        assertSame(codec, threadLocal.createCodec());

        verifyNoMoreInteractions(codecMock, factoryMock);

        codec.encode(new Object(), mock(DataWriter.class));

        verify(factoryMock).createCodec();
        verify(codecMock).encode(any(), any());
        verifyNoMoreInteractions(factoryMock, codecMock);
        clearInvocations(factoryMock, codecMock);

        codec.encode(new Object(), mock(DataWriter.class));

        verify(codecMock).encode(any(), any());
        verifyNoMoreInteractions(factoryMock, codecMock);
    }

    @Test
    public void testDoNotWrapIfAlreadyWrapped() {
        CodecFactory<Object> wrap = ThreadLocalCodecFactory.tryWrap(factoryMock);

        assertTrue(wrap instanceof ThreadLocalCodecFactory);

        assertSame(wrap, ThreadLocalCodecFactory.tryWrap(wrap));
    }

    @Test
    public void testDoNotWrapIfStatefulCodec() {
        when(codecMock.isStateful()).thenReturn(true);

        CodecFactory<Object> wrap = ThreadLocalCodecFactory.tryWrap(factoryMock);

        assertSame(factoryMock, wrap);
    }

    @Test
    public void testUnwrap() {
        JdkCodecFactory<Object> factory = new JdkCodecFactory<>();

        CodecFactory<Object> wrapped = ThreadLocalCodecFactory.tryWrap(factory);

        assertTrue(wrapped instanceof ThreadLocalCodecFactory);

        assertSame(factory, ThreadLocalCodecFactory.tryUnwrap(wrapped));

        assertNull(ThreadLocalCodecFactory.tryUnwrap(null));
    }

    @Test
    public void testToString() {
        CodecFactory<Object> factory = ThreadLocalCodecFactory.tryWrap(factoryMock);

        assertEquals(ThreadLocalCodecFactory.class.getSimpleName() + "[delegate=" + factoryMock + ']', factory.toString());
    }
}
