/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.cluster.seed;

import io.hekate.core.HekateException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class SeedNodeProviderAdaptor implements SeedNodeProvider {
    @Override
    public List<InetSocketAddress> getSeedNodes(String cluster) throws HekateException {
        return Collections.emptyList();
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public long getCleanupInterval() {
        return 0;
    }

    @Override
    public void registerRemoteAddress(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void unregisterRemoteAddress(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }
}
