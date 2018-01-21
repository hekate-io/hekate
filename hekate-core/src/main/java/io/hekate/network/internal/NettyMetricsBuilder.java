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

package io.hekate.network.internal;

import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.network.netty.NettyMetricsFactory;
import io.hekate.network.netty.NettyMetricsSink;

class NettyMetricsBuilder {
    private static final String METRIC_CONN_ACTIVE = "conn.active";

    private static final String METRIC_MSG_ERR = "msg.err";

    private static final String METRIC_MSG_QUEUE = "msg.queue";

    private static final String METRIC_MSG_OUT = "msg.out";

    private static final String METRIC_MSG_IN = "msg.in";

    private static final String METRIC_BYTES_IN = "bytes.in";

    private static final String METRIC_BYTES_OUT = "bytes.out";

    private final LocalMetricsService metrics;

    public NettyMetricsBuilder(LocalMetricsService metrics) {
        assert metrics != null : "Metrics service is null.";

        this.metrics = metrics;
    }

    public NettyMetricsFactory createServerFactory() {
        return doCreateFactory(true);
    }

    public NettyMetricsFactory createClientFactory() {
        return doCreateFactory(false);
    }

    private NettyMetricsFactory doCreateFactory(boolean server) {
        // Overall bytes.
        CounterMetric allBytesSent = counter(METRIC_BYTES_OUT, true);
        CounterMetric allBytesReceived = counter(METRIC_BYTES_IN, true);

        // Overall messages.
        CounterMetric allMsgSent = counter(METRIC_MSG_OUT, true);
        CounterMetric allMsgReceived = counter(METRIC_MSG_IN, true);
        CounterMetric allMsgQueue = counter(METRIC_MSG_QUEUE, false);
        CounterMetric allMsgFailed = counter(METRIC_MSG_ERR, true);

        // Overall connections.
        CounterMetric allConnections = counter(METRIC_CONN_ACTIVE, false);

        return protocol -> {
            // Connector bytes.
            CounterMetric bytesSent = counter(METRIC_BYTES_OUT, protocol, server, true);
            CounterMetric bytesReceived = counter(METRIC_BYTES_IN, protocol, server, true);

            // Connector messages.
            CounterMetric msgSent = counter(METRIC_MSG_OUT, protocol, server, true);
            CounterMetric msgReceived = counter(METRIC_MSG_IN, protocol, server, true);
            CounterMetric msgQueue = counter(METRIC_MSG_QUEUE, protocol, server, false);
            CounterMetric msgFailed = counter(METRIC_MSG_ERR, protocol, server, true);

            // Connector connections.
            CounterMetric connections = counter(METRIC_CONN_ACTIVE, protocol, server, false);

            return new NettyMetricsSink() {
                @Override
                public void onBytesSent(long bytes) {
                    bytesSent.add(bytes);

                    allBytesSent.add(bytes);
                }

                @Override
                public void onBytesReceived(long bytes) {
                    bytesReceived.add(bytes);

                    allBytesReceived.add(bytes);
                }

                @Override
                public void onMessageSent() {
                    msgSent.increment();

                    allMsgSent.increment();
                }

                @Override
                public void onMessageReceived() {
                    msgReceived.increment();

                    allMsgReceived.increment();
                }

                @Override
                public void onMessageSendError() {
                    msgFailed.increment();

                    allMsgFailed.increment();
                }

                @Override
                public void onMessageEnqueue() {
                    msgQueue.increment();

                    allMsgQueue.increment();
                }

                @Override
                public void onMessageDequeue() {
                    msgQueue.decrement();

                    allMsgQueue.decrement();
                }

                @Override
                public void onConnect() {
                    connections.increment();

                    allConnections.increment();
                }

                @Override
                public void onDisconnect() {
                    connections.decrement();

                    allConnections.decrement();
                }
            };
        };
    }

    private CounterMetric counter(String name, boolean autoReset) {
        return counter(name, null, null, autoReset);
    }

    private CounterMetric counter(String name, String protocol, Boolean server, boolean autoReset) {
        String counterName = "";

        if (protocol != null) {
            counterName += protocol + '.';
        } else {
            counterName += "hekate.";
        }

        counterName += "network.";

        if (server != null) {
            counterName += server ? "server." : "client.";
        }

        counterName += name;

        CounterConfig cfg = new CounterConfig(counterName);

        cfg.setAutoReset(autoReset);

        if (autoReset) {
            cfg.setName(counterName + ".current");
            cfg.setTotalName(counterName + ".total");
        } else {
            cfg.setName(counterName);
        }

        return metrics.register(cfg);
    }
}
