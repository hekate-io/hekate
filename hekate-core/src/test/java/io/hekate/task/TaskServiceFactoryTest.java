/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.task;

import io.hekate.HekateTestBase;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.JdkCodecFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TaskServiceFactoryTest extends HekateTestBase {
    private final TaskServiceFactory factory = new TaskServiceFactory();

    @Test
    public void testLocalExecutionEnabled() {
        assertTrue(factory.isLocalExecutionEnabled());

        factory.setLocalExecutionEnabled(false);

        assertFalse(factory.isLocalExecutionEnabled());

        assertSame(factory, factory.withLocalExecutionEnabled(true));

        assertTrue(factory.isLocalExecutionEnabled());
    }

    @Test
    public void testWorkerThreads() {
        assertEquals(Runtime.getRuntime().availableProcessors(), factory.getWorkerThreads());

        factory.setWorkerThreads(10000);

        assertEquals(10000, factory.getWorkerThreads());

        assertSame(factory, factory.withWorkerThreads(20000));

        assertEquals(20000, factory.getWorkerThreads());
    }

    @Test
    public void testTaskCodec() {
        assertNull(factory.getTaskCodec());

        CodecFactory<Object> codec = new JdkCodecFactory<>();

        factory.setTaskCodec(codec);

        assertSame(codec, factory.getTaskCodec());

        factory.setTaskCodec(null);

        assertNull(factory.getTaskCodec());

        assertSame(factory, factory.withTaskCodec(codec));

        assertSame(codec, factory.getTaskCodec());
    }

    @Test
    public void testToString() {
        assertTrue(factory.toString(), factory.toString().startsWith(TaskServiceFactory.class.getSimpleName()));
    }
}
