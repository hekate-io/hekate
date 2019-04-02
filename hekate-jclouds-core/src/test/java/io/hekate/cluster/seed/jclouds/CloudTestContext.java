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

package io.hekate.cluster.seed.jclouds;

import io.hekate.HekateTestProps;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CloudTestContext {
    private final String computeProvider;

    private final String storeProvider;

    private final String region;

    private final String autoCreateRegion;

    private final String storeBucket;

    @ToStringIgnore
    private final String identity;

    @ToStringIgnore
    private final String credential;

    @ToStringIgnore
    private final String[] allRegions;

    public CloudTestContext(
        String computeProvider,
        String storeProvider,
        String region,
        String autoCreateRegion,
        String storeBucket,
        String identity,
        String credential,
        String[] allRegions
    ) {
        this.computeProvider = computeProvider;
        this.storeProvider = storeProvider;
        this.region = region;
        this.autoCreateRegion = autoCreateRegion;
        this.storeBucket = storeBucket;
        this.identity = identity;
        this.credential = credential;
        this.allRegions = allRegions;
    }

    public static Collection<CloudTestContext> allContexts() {
        List<CloudTestContext> contexts = new ArrayList<>();

        if (HekateTestProps.is("AWS_TEST_ENABLED")) {
            contexts.add(new CloudTestContext(
                "aws-ec2",
                "aws-s3",
                HekateTestProps.get("AWS_TEST_REGION"),
                HekateTestProps.get("AWS_TEST_REGION"),
                HekateTestProps.get("AWS_TEST_BUCKET"),
                HekateTestProps.get("AWS_TEST_ACCESS_KEY"),
                HekateTestProps.get("AWS_TEST_SECRET_KEY"),
                new String[]{
                    "us-east-1",
                    "us-east-2",
                    "us-west-1",
                    "us-west-2"
                }
            ));
        }

        if (HekateTestProps.is("GOOGLE_TEST_ENABLED")) {
            contexts.add(new CloudTestContext(
                "google-compute-engine",
                "google-cloud-storage",
                HekateTestProps.get("GOOGLE_TEST_REGION"),
                HekateTestProps.get("GOOGLE_TEST_REGION") + "-a", // <-- Zone (region-name-a)
                HekateTestProps.get("GOOGLE_TEST_BUCKET"),
                HekateTestProps.get("GOOGLE_TEST_EMAIL"),
                HekateTestProps.get("GOOGLE_TEST_PRIVATE_KEY"),
                new String[]{
                    "europe-north1",
                    "europe-west1",
                    "europe-west2",
                    "europe-west3"
                }
            ));
        }

        return contexts;
    }

    public String computeProvider() {
        return computeProvider;
    }

    public String storeProvider() {
        return storeProvider;
    }

    public String region() {
        return region;
    }

    public String autoCreateRegion() {
        return autoCreateRegion;
    }

    public String storeBucket() {
        return storeBucket;
    }

    public String identity() {
        return identity;
    }

    public String credential() {
        return credential;
    }

    public String[] allRegions() {
        return allRegions;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
