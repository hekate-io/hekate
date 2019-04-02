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

package io.hekate.test;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.util.format.ToString;

public class SplitBrainDetectorMock implements SplitBrainDetector {
    private boolean valid;

    private int checks;

    public SplitBrainDetectorMock(boolean valid) {
        this.valid = valid;
    }

    @Override
    public synchronized boolean isValid(ClusterNode localNode) {
        checks++;

        return valid;
    }

    public synchronized void setValid(boolean valid) {
        checks = 0;

        this.valid = valid;
    }

    public void awaitForChecks(int checks) throws Exception {
        HekateTestBase.busyWait("checks " + checks, () -> {
            synchronized (this) {
                return this.checks >= checks;
            }
        });
    }

    public synchronized int getChecks() {
        return checks;
    }

    @Override
    public synchronized String toString() {
        return ToString.format(this);
    }
}
