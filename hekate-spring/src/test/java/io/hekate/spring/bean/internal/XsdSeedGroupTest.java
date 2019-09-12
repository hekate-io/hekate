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

import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.SeedNodeProviderGroupPolicy;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.core.Hekate;
import java.io.File;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.FileSystemUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath*:xsd-test/test-seed-group.xml")
public class XsdSeedGroupTest extends XsdTestBase {
    @Autowired
    @Qualifier("seedNodeTempDir")
    private String dirPath;

    @After
    public void tearDown() {
        FileSystemUtils.deleteRecursively(new File(dirPath));
    }

    @Test
    public void testExpectedProvider() {
        Hekate node = spring.getBean("node1", Hekate.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)node.get(DefaultClusterService.class).seedNodeProvider();

        assertSame(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS, group.policy());
        assertEquals(2, group.liveProviders().size());
        assertEquals(group.allProviders(), group.liveProviders());

        assertTrue(group.findProvider(FsSeedNodeProvider.class).isPresent());
        assertTrue(group.findProvider(StaticSeedNodeProvider.class).isPresent());
    }
}
