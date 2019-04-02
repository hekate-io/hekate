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

import com.esotericsoftware.kryo.Kryo;
import io.hekate.HekateTestBase;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.util.format.ToString;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;
import org.nustaq.serialization.FSTConfiguration;

import static org.junit.Assert.assertEquals;

public class AutoSelectCodecFactoryTest extends HekateTestBase {
    private static class ExclusiveClassLoader extends URLClassLoader {
        private static final URL[] EMPTY_URLS = new URL[0];

        private final List<String> excluded;

        public ExclusiveClassLoader(List<String> excluded, ClassLoader parent) {
            super(EMPTY_URLS, parent);

            this.excluded = excluded;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (excluded.contains(name)) {
                throw new ClassNotFoundException(name);
            }

            return super.loadClass(name, resolve);
        }
    }

    private static final String KRYO_CLASS = Kryo.class.getName();

    private static final String FST_CLASS = FSTConfiguration.class.getName();

    @Test
    public void testKryo() throws Exception {
        AutoSelectCodecFactory<Object> factory = new AutoSelectCodecFactory<>();

        assertEquals(KryoCodecFactory.class, factory.selected().getClass());
    }

    @Test
    public void testFst() throws Exception {
        ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(new ExclusiveClassLoader(Collections.singletonList(KRYO_CLASS), oldLoader));

            AutoSelectCodecFactory<Object> factory = new AutoSelectCodecFactory<>();

            assertEquals(FstCodecFactory.class, factory.selected().getClass());
        } finally {
            Thread.currentThread().setContextClassLoader(oldLoader);
        }
    }

    @Test
    public void testJdk() throws Exception {
        ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(new ExclusiveClassLoader(Arrays.asList(KRYO_CLASS, FST_CLASS), oldLoader));

            AutoSelectCodecFactory<Object> factory = new AutoSelectCodecFactory<>();

            assertEquals(JdkCodecFactory.class, factory.selected().getClass());
        } finally {
            Thread.currentThread().setContextClassLoader(oldLoader);
        }
    }

    @Test
    public void testToString() throws Exception {
        AutoSelectCodecFactory<Object> factory = new AutoSelectCodecFactory<>();

        assertEquals(ToString.format(factory), factory.toString());
    }
}
