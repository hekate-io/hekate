/*
 * Copyright 2021 The Hekate Project
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
import io.hekate.cluster.seed.consul.ConsulSeedNodeProvider;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath*:xsd-test/test-seed-consul.xml")
public class XsdSeedConsulTest extends XsdTestBase {
    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("CONSUL_ENABLED"));
    }

    @Test
    public void testExpectedProvider() throws Exception {
        assertFalse(spring.getBeansOfType(ConsulSeedNodeProvider.class).isEmpty());

        spring.getBeansOfType(ConsulSeedNodeProvider.class).values().forEach(provider -> {
            assertEquals("hekate-test", provider.basePath());
            assertEquals(HekateTestProps.get("CONSUL_URL"), provider.url().toString());
            assertEquals(3001, provider.connectTimeout().longValue());
            assertEquals(3002, provider.readTimeout().longValue());
            assertEquals(3003, provider.writeTimeout().longValue());
        });
    }
}
