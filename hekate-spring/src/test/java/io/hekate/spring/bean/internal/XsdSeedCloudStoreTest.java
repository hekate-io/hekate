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
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProviderConfig;
import java.util.Map;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath*:xsd-test/test-seed-cloud-store.xml")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD) // <-- Unique cluster ID must be generated for each method.
public class XsdSeedCloudStoreTest extends XsdTestBase {
    @Autowired
    private String testIdentity;

    @Autowired
    private String testCredential;

    @Autowired
    private String testContainer;

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Test
    public void testExpectedProvider() {
        assertFalse(spring.getBeansOfType(CloudStoreSeedNodeProvider.class).isEmpty());

        Map<String, CloudStoreSeedNodeProviderConfig> configs = spring.getBeansOfType(CloudStoreSeedNodeProviderConfig.class);

        assertFalse(configs.isEmpty());

        configs.values().forEach(cfg -> {
            assertEquals("aws-s3", cfg.getProvider());
            assertEquals(testContainer, cfg.getContainer());
            assertEquals(testIdentity, cfg.getCredentials().get().identity);
            assertEquals(testCredential, cfg.getCredentials().get().credential);
            assertEquals("test.value", cfg.getProperties().getProperty("test.property"));
        });
    }
}
