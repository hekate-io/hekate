/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.metrics.statsd;

import io.hekate.core.internal.HekateTestNode;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.metrics.local.ProbeConfig;
import java.net.InetAddress;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class StatsdMetricsPluginTest extends StatsdMetricsTestBase {
    @Test
    public void test() throws Exception {
        CountDownLatch published = new CountDownLatch(1);

        String localhost = InetAddress.getLocalHost().getHostAddress();

        HekateTestNode node = createNode(boot -> {
            boot.withPlugin(new StatsdMetricsPlugin(new StatsdMetricsConfig()
                .withHost(localhost)
                .withPort(testPort)
                .withFilter(metric -> metric.name().equals("test-metric")))
            );

            boot.withService(LocalMetricsServiceFactory.class)
                .withRefreshInterval(100)
                .withMetric(new ProbeConfig("test-metric")
                    .withProbe(() -> 1000)
                )
                .withListener(event -> published.countDown());
        }).join();

        await(published);

        String nodeHost = StatsdMetricsPublisher.toSafeHost(node.localNode().socket().getAddress().getHostAddress());
        int nodePort = node.localNode().socket().getPort();

        busyWait("metrics", () -> {
            String received = receiveNext();

            return received.contains(nodeHost + "__" + nodePort + ".test_metric:" + 1000 + "|g");
        });

        node.leave();
    }

    @Test
    public void testToString() {
        StatsdMetricsPlugin plugin = new StatsdMetricsPlugin(new StatsdMetricsConfig().withHost("localhost"));

        assertTrue(plugin.toString(), plugin.toString().startsWith(StatsdMetricsPlugin.class.getSimpleName()));
    }
}
