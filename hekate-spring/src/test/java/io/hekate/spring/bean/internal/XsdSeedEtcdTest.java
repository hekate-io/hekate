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

package io.hekate.spring.bean.internal;

import io.hekate.HekateTestProps;
import io.hekate.cluster.seed.etcd.EtcdSeedNodeProvider;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath*:xsd-test/test-seed-etcd.xml")
public class XsdSeedEtcdTest extends XsdTestBase {
    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("ETCD_ENABLED"));
    }

    @Test
    public void testExpectedProvider() {
        assertFalse(spring.getBeansOfType(EtcdSeedNodeProvider.class).isEmpty());

        spring.getBeansOfType(EtcdSeedNodeProvider.class).values().forEach(provider -> {
            assertEquals("/hekate-test", provider.basePath());
            assertEquals(2, provider.endpoints().size());
        });
    }
}
