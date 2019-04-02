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

import io.hekate.HekateTestBase;
import io.hekate.HekateTestProps;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProvider;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProviderConfig;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@ContextConfiguration("classpath*:xsd-test/test-seed-cloud.xml")
public class XsdSeedCloudTest extends HekateTestBase {
    @Autowired(required = false)
    private CloudSeedNodeProvider provider;

    @Autowired(required = false)
    private CloudSeedNodeProviderConfig providerCfg;

    @Autowired
    private String testIdentity;

    @Autowired
    private String testEndpoint;

    @Autowired
    private String testCredential;

    @Autowired
    private String testRegion;

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Test
    public void testExpectedProvider() {
        assertNotNull(provider);
        assertNotNull(providerCfg);

        assertEquals("aws-ec2", providerCfg.getProvider());
        assertEquals(testEndpoint, providerCfg.getEndpoint());
        assertEquals(testIdentity, providerCfg.getCredentials().get().identity);
        assertEquals(testCredential, providerCfg.getCredentials().get().credential);
        assertTrue(providerCfg.getRegions().contains(testRegion));
        assertTrue(providerCfg.getZones().contains(testRegion + 'a'));
        assertTrue(providerCfg.getZones().contains(testRegion + 'b'));
        assertEquals("1", providerCfg.getTags().get("HEKATE"));
        assertEquals("test.value", providerCfg.getProperties().getProperty("test.property"));
    }

    @Override
    protected void checkGhostThreads() throws InterruptedException {
        // Do not check threads since Spring context gets terminated after all tests have been run.
    }
}
