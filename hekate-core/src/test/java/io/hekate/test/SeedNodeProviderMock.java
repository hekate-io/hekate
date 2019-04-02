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

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class SeedNodeProviderMock implements SeedNodeProvider {
    private final ConcurrentHashMap<String, List<InetSocketAddress>> nodes = new ConcurrentHashMap<>();

    private volatile SeedNodeProvider delegate;

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        if (delegate != null) {
            return delegate.findSeedNodes(cluster);
        }

        List<InetSocketAddress> clusterNodes = nodes.get(cluster);

        return clusterNodes != null ? new ArrayList<>(clusterNodes) : null;
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        if (delegate != null) {
            delegate.startDiscovery(cluster, node);
        }

        List<InetSocketAddress> clusterNodes = nodes.get(cluster);

        if (clusterNodes == null) {
            clusterNodes = new CopyOnWriteArrayList<>();

            List<InetSocketAddress> existing = nodes.putIfAbsent(cluster, clusterNodes);

            if (existing != null) {
                clusterNodes = existing;
            }
        }

        clusterNodes.add(node);
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        if (delegate != null) {
            delegate.suspendDiscovery();
        }
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        List<InetSocketAddress> clusterNodes = nodes.get(cluster);

        if (clusterNodes != null) {
            clusterNodes.remove(node);
        }

        if (delegate != null) {
            delegate.stopDiscovery(cluster, node);
        }
    }

    @Override
    public long cleanupInterval() {
        return delegate != null ? delegate.cleanupInterval() : 0;
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        if (delegate != null) {
            delegate.registerRemote(cluster, node);
        }
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        if (delegate != null) {
            delegate.unregisterRemote(cluster, node);
        }
    }

    public void setDelegate(SeedNodeProvider delegate) {
        this.delegate = delegate;
    }
}
