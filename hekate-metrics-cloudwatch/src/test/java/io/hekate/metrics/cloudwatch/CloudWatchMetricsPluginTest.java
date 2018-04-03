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

package io.hekate.metrics.cloudwatch;

import com.amazonaws.http.IdleConnectionReaper;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import io.hekate.HekateNodeTestBase;
import io.hekate.HekateTestProps;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsPublisher.CloudWatchClient;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.metrics.local.ProbeConfig;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CloudWatchMetricsPluginTest extends HekateNodeTestBase {
    @BeforeClass
    public static void setUpClass() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Override
    @After
    public void tearDown() throws Exception {
        IdleConnectionReaper.shutdown();

        super.tearDown();
    }

    @Test
    public void test() throws Exception {
        String awsAccessKey = HekateTestProps.get("AWS_TEST_ACCESS_KEY");
        String awsSecretKey = HekateTestProps.get("AWS_TEST_SECRET_KEY");
        String awsRegion = HekateTestProps.get("AWS_TEST_REGION");

        int publishIntervalSeconds = 1;

        CloudWatchClientRequestCaptor captor = new CloudWatchClientRequestCaptor();

        CountDownLatch published = new CountDownLatch(1);

        createNode(boot -> {
            boot.withPlugin(new CloudWatchMetricsPlugin(new CloudWatchMetricsConfig()
                .withPublishInterval(10)
                .withRegion(awsRegion)
                .withMetaDataProvider(new CloudWatchMetaDataProviderMock())
                .withAccessKey(awsAccessKey)
                .withSecretKey(awsSecretKey)
                .withPublishInterval(publishIntervalSeconds)
                .withFilter(m -> m.name().equals("test-metric"))
            ) {
                @Override
                CloudWatchClient buildCloudWatchClient(AmazonCloudWatch cloudWatch) {
                    // Supersede the real client with a mock.
                    return captor;
                }
            });

            boot.withService(LocalMetricsServiceFactory.class)
                .withRefreshInterval(100)
                .withMetric(new ProbeConfig("test-metric")
                    .withProbe(() -> 1000)
                )
                .withListener(event -> published.countDown());
        }).join();

        busyWait("metrics published", () -> !captor.requests().isEmpty());

        assertEquals(1, captor.requests().size());

        PutMetricDataRequest request = captor.requests().get(0);

        assertEquals(1, request.getMetricData().size());
        assertEquals("TestMetric", request.getMetricData().get(0).getMetricName());
    }
}
