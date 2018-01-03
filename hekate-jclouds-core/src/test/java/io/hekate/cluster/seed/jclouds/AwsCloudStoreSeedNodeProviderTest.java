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

package io.hekate.cluster.seed.jclouds;

import io.hekate.HekateTestProps;
import io.hekate.cluster.seed.PersistentSeedNodeProviderTestBase;
import io.hekate.util.format.ToString;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AwsCloudStoreSeedNodeProviderTest extends PersistentSeedNodeProviderTestBase<CloudStoreSeedNodeProvider> {
    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(HekateTestProps.is("AWS_TEST_ENABLED"));
    }

    @Test
    public void testToString() throws Exception {
        CloudStoreSeedNodeProvider provider = createProvider();

        assertEquals(ToString.format(provider), provider.toString());
    }

    @Override
    protected CloudStoreSeedNodeProvider createProvider() throws Exception {
        CloudStoreSeedNodeProviderConfig cfg = new CloudStoreSeedNodeProviderConfig()
            .withProvider("aws-s3")
            .withContainer(HekateTestProps.get("AWS_TEST_BUCKET"))
            .withCredentials(new BasicCredentialsSupplier()
                .withIdentity(HekateTestProps.get("AWS_TEST_ACCESS_KEY"))
                .withCredential(HekateTestProps.get("AWS_TEST_SECRET_KEY"))
            )
            .withCleanupInterval(100);

        return new CloudStoreSeedNodeProvider(cfg);
    }
}
