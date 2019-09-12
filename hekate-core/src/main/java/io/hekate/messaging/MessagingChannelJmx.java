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

package io.hekate.messaging;

import io.hekate.cluster.ClusterNodeJmx;
import io.hekate.core.jmx.JmxTypeName;
import javax.management.MXBean;

/**
 * JMX interface for {@link MessagingChannel}.
 */
@MXBean
@JmxTypeName("MessagingChannel")
public interface MessagingChannelJmx {
    /**
     * See {@link MessagingChannel#id()}.
     *
     * @return {@link MessagingChannel#id()}
     */
    String getId();

    /**
     * See {@link MessagingChannel#name()}.
     *
     * @return {@link MessagingChannel#id()}.
     */
    String getName();

    /**
     * See {@link MessagingChannel#baseType()}.
     *
     * @return {@link MessagingChannel#baseType()} as a string.
     */
    String getBaseType();

    /**
     * See {@link MessagingChannel#nioThreads()}.
     *
     * @return {@link MessagingChannel#nioThreads()}.
     */
    int getNioThreads();

    /**
     * See {@link MessagingChannel#workerThreads()}.
     *
     * @return {@link MessagingChannel#workerThreads()}.
     */
    int getWorkerThreads();

    /**
     * Returns the cluster topology of this channel (see {@link MessagingChannel#cluster()}).
     *
     * @return Cluster topology of this channel.
     */
    ClusterNodeJmx[] getTopology();

    /**
     * Returns the value of {@link MessagingChannelConfig#setPartitions(int)}.
     *
     * @return Value of {@link MessagingChannelConfig#setPartitions(int)}
     */
    int getPartitions();

    /**
     * Returns the value of {@link MessagingChannelConfig#setBackupNodes(int)}.
     *
     * @return Value of {@link MessagingChannelConfig#setBackupNodes(int)}.
     */
    int getBackupNodes();

    /**
     * Returns the value of {@link MessagingChannelConfig#setIdleSocketTimeout(long)}.
     *
     * @return Value of {@link MessagingChannelConfig#setIdleSocketTimeout(long)}.
     */
    long getIdleSocketTimeout();

    /**
     * Returns {@code true} if this channel has a {@link MessageReceiver}.
     *
     * @return {@code true} if this channel has a {@link MessageReceiver}.
     *
     * @see MessagingChannelConfig#setReceiver(MessageReceiver)
     */
    boolean isReceiver();

    /**
     * Returns the value of {@link MessagingChannelConfig#setLogCategory(String)}.
     *
     * @return Value of {@link MessagingChannelConfig#setLogCategory(String)}.
     */
    String getLogCategory();

    /**
     * Returns the value of the channel's {@link MessagingBackPressureConfig#setInLowWatermark(int)} or {@code null} if inbound back
     * pressure is disabled.
     *
     * @return Value of the channel's {@link MessagingBackPressureConfig#setInLowWatermark(int)} or {@code null} if inbound back pressure is
     * disabled.
     *
     * @see MessagingChannelConfig#setBackPressure(MessagingBackPressureConfig)
     */
    Integer getBackPressureInLowWatermark();

    /**
     * Returns the value of the channel's {@link MessagingBackPressureConfig#setInHighWatermark(int)} or {@code null} if inbound back
     * pressure is disabled.
     *
     * @return Value of the channel's {@link MessagingBackPressureConfig#setInHighWatermark(int)} or {@code null} if inbound back pressure
     * is disabled.
     *
     * @see MessagingChannelConfig#setBackPressure(MessagingBackPressureConfig)
     */
    Integer getBackPressureInHighWatermark();

    /**
     * Returns the value of the channel's {@link MessagingBackPressureConfig#setOutLowWatermark(int)} or {@code null} if outbound back
     * pressure is disabled.
     *
     * @return Value of the channel's {@link MessagingBackPressureConfig#setOutLowWatermark(int)} or {@code null} if outbound back pressure
     * is disabled.
     *
     * @see MessagingChannelConfig#setBackPressure(MessagingBackPressureConfig)
     */
    Integer getBackPressureOutLowWatermark();

    /**
     * Returns the value of the channel's {@link MessagingBackPressureConfig#setOutHighWatermark(int)} or {@code null} if outbound back
     * pressure is disabled.
     *
     * @return Value of the channel's {@link MessagingBackPressureConfig#setOutHighWatermark(int)} or {@code null} if outbound back pressure
     * is disabled.
     *
     * @see MessagingChannelConfig#setBackPressure(MessagingBackPressureConfig)
     */
    Integer getBackPressureOutHighWatermark();

    /**
     * Returns the value of the channel's {@link MessagingBackPressureConfig#setOutOverflowPolicy(MessagingOverflowPolicy)} or {@code null}
     * if outbound back pressure is disabled.
     *
     * @return Value of the channel's {@link MessagingBackPressureConfig#setOutOverflowPolicy(MessagingOverflowPolicy)} or {@code null}
     * if outbound back pressure is disabled.
     *
     * @see MessagingChannelConfig#setBackPressure(MessagingBackPressureConfig)
     */
    MessagingOverflowPolicy getBackPressureOutOverflowPolicy();
}
