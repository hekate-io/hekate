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

package io.hekate.spring.boot.codec;

import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.codec.AutoSelectCodecFactory;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HekateCodecConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class TestConfig extends HekateTestConfigBase {
        // No-op.
    }

    @Test
    public void testDefault() throws Exception {
        registerAndRefresh(TestConfig.class);

        assertEquals(AutoSelectCodecFactory.class, get(HekateBootstrap.class).getDefaultCodec().getClass());
    }

    @Test
    public void testKryo() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.codec=kryo",
            "hekate.codec.kryo.knownTypes=" + DefaultClusterNode.class.getName() + ", " + DefaultClusterTopology.class.getName(),
        }, TestConfig.class);

        KryoCodecFactory<Object> codec = (KryoCodecFactory<Object>)get(HekateBootstrap.class).getDefaultCodec();

        assertTrue(codec.getKnownTypes().contains(DefaultClusterNode.class));
        assertTrue(codec.getKnownTypes().contains(DefaultClusterTopology.class));
    }

    @Test
    public void testFst() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.codec=fst",
            "hekate.codec.fst.knownTypes=" + DefaultClusterNode.class.getName() + ", " + DefaultClusterTopology.class.getName(),
        }, TestConfig.class);

        FstCodecFactory<Object> codec = (FstCodecFactory<Object>)get(HekateBootstrap.class).getDefaultCodec();

        assertTrue(codec.getKnownTypes().contains(DefaultClusterNode.class));
        assertTrue(codec.getKnownTypes().contains(DefaultClusterTopology.class));
    }

    @Test
    public void testJdk() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.codec=jdk",
        }, TestConfig.class);

        assertEquals(JdkCodecFactory.class, get(HekateBootstrap.class).getDefaultCodec().getClass());
    }
}
