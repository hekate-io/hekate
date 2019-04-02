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

package io.hekate.network.internal;

import io.hekate.network.netty.NettyMetricsFactory;
import io.hekate.network.netty.NettyMetricsSink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.LongAdder;

class NettyMetricsBuilder {
    private final MeterRegistry metrics;

    public NettyMetricsBuilder(MeterRegistry metrics) {
        assert metrics != null : "Metrics registry is null.";

        this.metrics = metrics;
    }

    public NettyMetricsFactory createServerFactory() {
        return doCreateFactory();
    }

    public NettyMetricsFactory createClientFactory() {
        return doCreateFactory();
    }

    private NettyMetricsFactory doCreateFactory() {
        return protocol -> {
            // Bytes.
            Counter bytesSent = Counter.builder("hekate.network.bytes.out")
                .tag("protocol", protocol)
                .register(metrics);

            Counter bytesReceived = Counter.builder("hekate.network.bytes.in")
                .tag("protocol", protocol)
                .register(metrics);

            // Messages.
            Counter msgSent = Counter.builder("hekate.network.message.out")
                .tag("protocol", protocol)
                .register(metrics);

            Counter msgReceived = Counter.builder("hekate.network.message.in")
                .tag("protocol", protocol)
                .register(metrics);

            LongAdder msgQueue = new LongAdder();

            Gauge.builder("hekate.network.message.queue", msgQueue, LongAdder::doubleValue)
                .tag("protocol", protocol)
                .register(metrics);

            // Connections.
            Counter conns = Counter.builder("hekate.network.connection.count")
                .tag("protocol", protocol)
                .register(metrics);

            LongAdder connsAct = new LongAdder();

            Gauge.builder("hekate.network.connection.active", connsAct, LongAdder::doubleValue)
                .tag("protocol", protocol)
                .register(metrics);

            return new NettyMetricsSink() {
                @Override
                public void onBytesSent(long bytes) {
                    bytesSent.increment(bytes);
                }

                @Override
                public void onBytesReceived(long bytes) {
                    bytesReceived.increment(bytes);
                }

                @Override
                public void onMessageSent() {
                    msgSent.increment();
                }

                @Override
                public void onMessageReceived() {
                    msgReceived.increment();
                }

                @Override
                public void onMessageEnqueue() {
                    msgQueue.increment();
                }

                @Override
                public void onMessageDequeue() {
                    msgQueue.decrement();
                }

                @Override
                public void onConnect() {
                    connsAct.increment();

                    conns.increment();
                }

                @Override
                public void onDisconnect() {
                    connsAct.decrement();
                }
            };
        };
    }
}
