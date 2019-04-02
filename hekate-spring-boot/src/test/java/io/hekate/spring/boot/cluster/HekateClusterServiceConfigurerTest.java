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

package io.hekate.spring.boot.cluster;

import io.hekate.cluster.ClusterAcceptor;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.health.DefaultFailureDetectorConfig;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.SeedNodeProviderGroupPolicy;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.Hekate;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import io.hekate.test.TestUtils;
import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HekateClusterServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class DefaultTestConfig {
        // No-op.
    }

    @EnableAutoConfiguration
    public static class ClusterListenerTestConfig extends HekateTestConfigBase {
        private final AtomicInteger fired = new AtomicInteger();

        @Autowired
        private ClusterService clusterService;

        @Bean
        public ClusterEventListener listener() {
            return event -> fired.incrementAndGet();
        }
    }

    @EnableAutoConfiguration
    public static class FailureDetectorConfigTestConfig extends HekateTestConfigBase {
        private static final int HEARTBEAT_INTERVAL = 100500;

        @Bean
        public DefaultFailureDetectorConfig failureDetectorConfig() {
            return new DefaultFailureDetectorConfig().withHeartbeatInterval(HEARTBEAT_INTERVAL);
        }
    }

    @EnableAutoConfiguration
    public static class FailureDetectorTestConfig extends HekateTestConfigBase {
        private static class TestFailureDetector extends DefaultFailureDetector {
            // No-op.
        }

        @Bean
        public FailureDetector failureDetector() {
            return new TestFailureDetector();
        }
    }

    @EnableAutoConfiguration
    public static class JoinAcceptorsTestConfig extends HekateTestConfigBase {
        private static class TestAcceptor implements ClusterAcceptor {
            @Override
            public String acceptJoin(ClusterNode joining, Hekate local) {
                return null;
            }
        }

        @Bean
        public TestAcceptor acceptor() {
            return new TestAcceptor();
        }
    }

    @EnableAutoConfiguration
    public static class SplitBrainTestConfig extends HekateTestConfigBase {
        private static class TestDetector implements SplitBrainDetector {
            @Override
            public boolean isValid(ClusterNode localNode) {
                return true;
            }
        }

        @Bean
        public TestDetector detector() {
            return new TestDetector();
        }
    }

    private File tempDir;

    @Before
    public void setUp() throws Exception {
        tempDir = Files.createTempDirectory("hekate_fs_seed").toFile();
    }

    @Override
    public void tearDown() throws Exception {
        TestUtils.deleteDir(tempDir);

        super.tearDown();
    }

    @Test
    public void testClusterListener() {
        registerAndRefresh(ClusterListenerTestConfig.class);

        assertEquals(1, get(ClusterListenerTestConfig.class).fired.get());
        assertNotNull(get(ClusterListenerTestConfig.class).clusterService);
        assertNotNull(get("clusterService", ClusterService.class));
    }

    @Test
    public void testFailureDetectorConfig() {
        registerAndRefresh(FailureDetectorConfigTestConfig.class);

        DefaultFailureDetector detector = (DefaultFailureDetector)getNode().get(DefaultClusterService.class).failureDetector();

        assertEquals(FailureDetectorConfigTestConfig.HEARTBEAT_INTERVAL, detector.heartbeatInterval());
    }

    @Test
    public void testFailureDetector() {
        registerAndRefresh(FailureDetectorTestConfig.class);

        FailureDetector detector = getNode().get(DefaultClusterService.class).failureDetector();

        assertSame(FailureDetectorTestConfig.TestFailureDetector.class, detector.getClass());
    }

    @Test
    public void testAcceptors() {
        registerAndRefresh(JoinAcceptorsTestConfig.class);

        List<ClusterAcceptor> acceptors = getNode().get(DefaultClusterService.class).acceptors();

        assertTrue(acceptors.stream().anyMatch(v -> v instanceof JoinAcceptorsTestConfig.TestAcceptor));
    }

    @Test
    public void testSplitBrainDetector() {
        registerAndRefresh(new String[]{"hekate.cluster.split-brain-action=REJOIN"}, SplitBrainTestConfig.class);

        SplitBrainDetector detector = getNode().get(DefaultClusterService.class).splitBrainDetector();

        assertNotNull(detector);
        assertSame(SplitBrainTestConfig.TestDetector.class, detector.getClass());
        assertSame(SplitBrainAction.REJOIN, getNode().get(DefaultClusterService.class).splitBrainAction());
    }

    @Test
    public void testSeedNodeProviderGroup() {
        registerAndRefresh(new String[]{
            // Policy.
            "hekate.cluster.seed.policy=IGNORE_PARTIAL_ERRORS",
            // Static.
            "hekate.cluster.seed.static.enable=true",
            "hekate.cluster.seed.static.addresses=localhost:10012,localhost:10013",
            // Multicast.
            "hekate.cluster.seed.multicast.enable=true",
            "hekate.cluster.seed.multicast.interval=10",
            "hekate.cluster.seed.multicast.waitTime=20",
            // File system.
            "hekate.cluster.seed.filesystem.enable=true",
            "hekate.cluster.seed.filesystem.work-dir=" + tempDir.getAbsolutePath()
        }, DefaultTestConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertSame(SeedNodeProviderGroupPolicy.IGNORE_PARTIAL_ERRORS, group.policy());
        assertEquals(3, group.allProviders().size());

        assertTrue(group.findProvider(StaticSeedNodeProvider.class).isPresent());
        assertTrue(group.findProvider(MulticastSeedNodeProvider.class).isPresent());
        assertTrue(group.findProvider(FsSeedNodeProvider.class).isPresent());

        assertEquals(group.allProviders(), group.liveProviders());
    }
}
