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

package io.hekate.network.netty;

/**
 * Sink for metrics of a Netty-based network endpoint.
 *
 * @see NettyMetricsFactory
 * @see NettyClientFactory#setMetrics(NettyMetricsSink)
 */
public interface NettyMetricsSink {
    /**
     * Bytes sent.
     *
     * @param bytes Amount of sent bytes.
     */
    void onBytesSent(long bytes);

    /**
     * Bytes received.
     *
     * @param bytes Amount of received bytes.
     */
    void onBytesReceived(long bytes);

    /**
     * Message sent.
     */
    void onMessageSent();

    /**
     * Message received.
     */
    void onMessageReceived();

    /**
     * Connected.
     */
    void onConnect();

    /**
     * Disconnect.
     */
    void onDisconnect();

    /**
     * Message added to the queue.
     */
    void onMessageEnqueue();

    /**
     * Message removed from the queue.
     */
    void onMessageDequeue();
}
