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

package io.hekate.cluster.seed.multicast;

import io.hekate.HekateNodeTestBase;
import io.hekate.HekateTestProps;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;

public class MulticastSeedNodeProviderJmxTest extends HekateNodeTestBase {
    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("MULTICAST_ENABLED"));
    }

    @Test
    public void test() throws Exception {
        MulticastSeedNodeProviderConfig cfg = new MulticastSeedNodeProviderConfig()
            .withWaitTime(987)
            .withGroup("224.1.2.14")
            .withPort(MulticastSeedNodeProviderConfig.DEFAULT_PORT + 1)
            .withTtl(123)
            .withInterval(321);

        MulticastSeedNodeProvider seedNodeProvider = new MulticastSeedNodeProvider(cfg);

        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(ClusterServiceFactory.class, cluster ->
                cluster.withSeedNodeProvider(seedNodeProvider)
            );
        }).join();

        ObjectName name = node.get(JmxService.class).nameFor(MulticastSeedNodeProviderJmx.class);

        assertEquals('/' + cfg.getGroup() + ':' + cfg.getPort(), jmxAttribute(name, "Group", String.class, node));
        assertEquals(987, (long)jmxAttribute(name, "WaitTime", Long.class, node));
        assertEquals(123, (int)jmxAttribute(name, "Ttl", Integer.class, node));
        assertEquals(321, (long)jmxAttribute(name, "Interval", Long.class, node));
    }
}
