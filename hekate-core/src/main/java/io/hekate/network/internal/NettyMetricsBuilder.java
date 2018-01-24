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
    private static final String METRIC_CONNECTIONS = "connections";

    private static final String METRIC_CONNECTIONS_ACTIVE = "connections.active";

    private static final String METRIC_MSG_QUEUE = "messages.queue";

    private static final String METRIC_MSG_OUT = "messages.out";

    private static final String METRIC_MSG_IN = "messages.in";

    private static final String METRIC_BYTES_IN = "bytes.in";

    private static final String METRIC_BYTES_OUT = "bytes.out";

    private final LocalMetricsService metrics;

    public NettyMetricsBuilder(LocalMetricsService metrics) {
        assert metrics != null : "Metrics service is null.";

        this.metrics = metrics;
    }

    public NettyMetricsFactory createServerFactory() {
        return doCreateFactory();
    }

    public NettyMetricsFactory createClientFactory() {
        return doCreateFactory();
    }

    private NettyMetricsFactory doCreateFactory() {
        // Overall bytes.
        CounterMetric globalBytesSent = counter(METRIC_BYTES_OUT, true, true);
        CounterMetric globalBytesReceived = counter(METRIC_BYTES_IN, true, true);

        // Overall messages.
        CounterMetric globalMsgSent = counter(METRIC_MSG_OUT, true, true);
        CounterMetric globalMsgReceived = counter(METRIC_MSG_IN, true, true);
        CounterMetric globalMsgQueue = counter(METRIC_MSG_QUEUE, false, false);

        // Overall connections.
        CounterMetric globalConns = counter(METRIC_CONNECTIONS, false, true);
        CounterMetric globalConnsAct = counter(METRIC_CONNECTIONS_ACTIVE, false, false);

        return protocol -> {
            // Bytes.
            CounterMetric bytesSent = counter(METRIC_BYTES_OUT, protocol, true, true);
            CounterMetric bytesReceived = counter(METRIC_BYTES_IN, protocol, true, true);

            // Messages.
            CounterMetric msgQueue = counter(METRIC_MSG_QUEUE, protocol, false, false);
            CounterMetric msgSent = counter(METRIC_MSG_OUT, protocol, true, true);
            CounterMetric msgReceived = counter(METRIC_MSG_IN, protocol, true, true);

            // Connections.
            CounterMetric conns = counter(METRIC_CONNECTIONS, protocol, true, true);
            CounterMetric connsAct = counter(METRIC_CONNECTIONS_ACTIVE, protocol, false, false);

            return new NettyMetricsSink() {
                @Override
                public void onBytesSent(long bytes) {
                    bytesSent.add(bytes);

                    globalBytesSent.add(bytes);
                }

                @Override
                public void onBytesReceived(long bytes) {
                    bytesReceived.add(bytes);

                    globalBytesReceived.add(bytes);
                }

                @Override
                public void onMessageSent() {
                    msgSent.increment();

                    globalMsgSent.increment();
                }

                @Override
                public void onMessageReceived() {
                    msgReceived.increment();

                    globalMsgReceived.increment();
                }

                @Override
                public void onMessageEnqueue() {
                    msgQueue.increment();

                    globalMsgQueue.increment();
                }

                @Override
                public void onMessageDequeue() {
                    msgQueue.decrement();

                    globalMsgQueue.decrement();
                }

                @Override
                public void onConnect() {
                    connsAct.increment();
                    globalConnsAct.increment();

                    conns.increment();
                    globalConns.increment();
                }

                @Override
                public void onDisconnect() {
                    connsAct.decrement();
                    globalConnsAct.decrement();
                }
            };
        };
    }

    private CounterMetric counter(String name, boolean autoReset, boolean withTotal) {
        return counter(name, null, autoReset, withTotal);
    }

    private CounterMetric counter(String name, String protocol, boolean autoReset, boolean withTotal) {
        String counterName = "";

        if (protocol != null) {
            counterName += protocol + '.';
        } else {
            counterName += "hekate.";
        }

        counterName += "network.";

        counterName += name;

        CounterConfig cfg = new CounterConfig(counterName);

        cfg.setAutoReset(autoReset);

        if (withTotal) {
            cfg.setName(counterName + ".interim");
            cfg.setTotalName(counterName + ".total");
        } else {
            cfg.setName(counterName);
        }

        return metrics.register(cfg);
    }
}
