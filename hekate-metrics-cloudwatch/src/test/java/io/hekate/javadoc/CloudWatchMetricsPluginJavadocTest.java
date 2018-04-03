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

package io.hekate.javadoc;

import com.amazonaws.http.IdleConnectionReaper;
import io.hekate.HekateNodeTestBase;
import io.hekate.HekateTestProps;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.metrics.cloudwatch.CloudWatchMetaDataProviderMock;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsConfig;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsPlugin;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class CloudWatchMetricsPluginJavadocTest extends HekateNodeTestBase {
    @BeforeClass
    public static void setUpClass() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Test
    public void test() throws Exception {
        try {
            //Start:configure
            CloudWatchMetricsConfig cloudWatchCfg = new CloudWatchMetricsConfig()
                // Publishing interval in seconds
                .withPublishInterval(60)
                // AWS credentials (optional when running with a EC2 instance).
                .withRegion("eu-central-1")
                .withAccessKey("<my access key>")
                .withSecretKey("<my secret key>");
            //End:configure

            String awsAccessKey = HekateTestProps.get("AWS_TEST_ACCESS_KEY");
            String awsSecretKey = HekateTestProps.get("AWS_TEST_SECRET_KEY");
            String awsRegion = HekateTestProps.get("AWS_TEST_REGION");

            cloudWatchCfg.setPublishInterval(Integer.MAX_VALUE);
            cloudWatchCfg.setRegion(awsRegion);
            cloudWatchCfg.setAccessKey(awsAccessKey);
            cloudWatchCfg.setSecretKey(awsSecretKey);
            cloudWatchCfg.setMetaDataProvider(new CloudWatchMetaDataProviderMock());

            // Start:boot
            Hekate node = new HekateBootstrap()
                .withPlugin(new CloudWatchMetricsPlugin(cloudWatchCfg))
                .join();
            // End:boot

            node.leave();
        } finally {
            IdleConnectionReaper.shutdown();
        }
    }
}
