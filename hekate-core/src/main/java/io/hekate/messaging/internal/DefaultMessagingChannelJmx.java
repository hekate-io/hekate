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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNodeJmx;
import io.hekate.messaging.MessagingChannelJmx;
import io.hekate.messaging.MessagingOverflowPolicy;

class DefaultMessagingChannelJmx implements MessagingChannelJmx {
    private final MessagingGateway<?> gateway;

    public DefaultMessagingChannelJmx(MessagingGateway<?> gateway) {
        this.gateway = gateway;
    }

    @Override
    public String getId() {
        return gateway.channelId().toString();
    }

    @Override
    public String getName() {
        return gateway.name();
    }

    @Override
    public String getBaseType() {
        return gateway.baseType().getName();
    }

    @Override
    public int getNioThreads() {
        return gateway.nioThreads();
    }

    @Override
    public int getWorkerThreads() {
        return gateway.workerThreads();
    }

    @Override
    public ClusterNodeJmx[] getTopology() {
        return gateway.requireContext()
            .cluster()
            .topology().stream()
            .map(ClusterNodeJmx::of)
            .toArray(ClusterNodeJmx[]::new);
    }

    @Override
    public int getPartitions() {
        return gateway.partitions();
    }

    @Override
    public int getBackupNodes() {
        return gateway.backupNodes();
    }

    @Override
    public long getIdleSocketTimeout() {
        return gateway.idleSocketTimeout();
    }

    @Override
    public boolean isReceiver() {
        return gateway.hasReceiver();
    }

    @Override
    public String getLogCategory() {
        return gateway.logCategory();
    }

    @Override
    public Integer getBackPressureInLowWatermark() {
        ReceivePressureGuard guard = gateway.receivePressureGuard();

        return guard != null ? guard.loMark() : null;
    }

    @Override
    public Integer getBackPressureInHighWatermark() {
        ReceivePressureGuard guard = gateway.receivePressureGuard();

        return guard != null ? guard.hiMark() : null;
    }

    @Override
    public Integer getBackPressureOutLowWatermark() {
        SendPressureGuard guard = gateway.sendPressureGuard();

        return guard != null ? guard.loMark() : null;
    }

    @Override
    public Integer getBackPressureOutHighWatermark() {
        SendPressureGuard guard = gateway.sendPressureGuard();

        return guard != null ? guard.hiMark() : null;
    }

    @Override
    public MessagingOverflowPolicy getBackPressureOutOverflowPolicy() {
        SendPressureGuard guard = gateway.sendPressureGuard();

        return guard != null ? guard.policy() : null;
    }
}
