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

package io.hekate.core.internal;

import io.hekate.core.Hekate;
import io.hekate.core.HekateJmx;
import io.hekate.core.HekateVersion;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public class HekateNodeJmx implements HekateJmx {
    private final Hekate hekate;

    private volatile OptionalLong startTimeNanos = OptionalLong.empty();

    public HekateNodeJmx(Hekate hekate) {
        assert hekate != null : "Hekate instance is null.";

        this.hekate = hekate;

        hekate.addListener(changed -> {
            if (changed.state() == Hekate.State.UP) {
                startTimeNanos = OptionalLong.of(System.nanoTime());
            } else if (changed.state() == Hekate.State.DOWN) {
                startTimeNanos = OptionalLong.empty();
            }
        });
    }

    @Override
    public String getVersion() {
        return HekateVersion.fullVersion();
    }

    @Override
    public String getNodeName() {
        return hekate.localNode().name();
    }

    @Override
    public String getNodeId() {
        return hekate.localNode().id().toString();
    }

    @Override
    public String getClusterName() {
        return hekate.cluster().clusterName();
    }

    @Override
    public String getHost() {
        return hekate.localNode().address().socket().getAddress().getHostAddress();
    }

    @Override
    public int getPort() {
        return hekate.localNode().address().socket().getPort();
    }

    @Override
    public String getSocketAddress() {
        return hekate.localNode().address().socket().toString();
    }

    @Override
    public Hekate.State getState() {
        return hekate.state();
    }

    @Override
    public long getUpTimeMillis() {
        OptionalLong mayBeStarted = this.startTimeNanos;

        if (mayBeStarted.isPresent()) {
            return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - mayBeStarted.getAsLong());
        } else {
            return 0;
        }
    }

    @Override
    public String getUpTime() {
        Duration duration = Duration.ofMillis(getUpTimeMillis());

        return duration.toString();
    }
}
