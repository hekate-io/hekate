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

package io.hekate.javadoc.cluster;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterFilterSupport;
import org.junit.Test;

public class ClusterFilterSupportJavadocTest extends HekateTestBase {
    private static class FakeFilter implements ClusterFilterSupport<FakeFilter> {
        public void doSomething() {
            // No-op.
        }

        @Override
        public FakeFilter filterAll(ClusterFilter filter) {
            return this;
        }
    }

    @Test
    public void badAndGoodFilterExample() {
        FakeFilter cluster = new FakeFilter();
        FakeFilter filtered;

        // Start:bad_and_good_usage
        // Bad!!! (filtering is performed on every iteration).
        for (int i = 0; i < 1000; i++) {
            cluster.forRemotes().forRole("some_role").doSomething();
        }

        // Good (filtering is performed only once).
        filtered = cluster.forRemotes().forRole("some_role");
        for (int i = 0; i < 1000; i++) {
            filtered.doSomething();
        }
        // End:bad_and_good_usage
    }
}
