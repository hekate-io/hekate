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

package io.hekate.metrics.statsd;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricFilter;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StatsdMetricsPublisher {
    private static class QueueEntry {
        private final long timestamp;

        private final Metric[] metrics;

        public QueueEntry(long timestamp, Metric[] metrics) {
            this.timestamp = timestamp;
            this.metrics = metrics;
        }

        public long timestamp() {
            return timestamp;
        }

        public Metric[] metrics() {
            return metrics;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(StatsdMetricsPlugin.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final Pattern UNSAFE_HOST_CHARACTERS = Pattern.compile("[^a-z0-9]", Pattern.CASE_INSENSITIVE);

    private static final Pattern UNSAFE_CHARACTERS = Pattern.compile("[^a-z0-9.]", Pattern.CASE_INSENSITIVE);

    private static final QueueEntry STOP_ENTRY = new QueueEntry(0, new Metric[0]);

    private final String statsdHost;

    private final int statsdPort;

    private final int batchSize;

    private final int maxQueueSize;

    private final MetricFilter filter;

    @ToStringIgnore
    private final BlockingQueue<QueueEntry> queue;

    @ToStringIgnore
    private final Object mux = new Object();

    @ToStringIgnore
    private ExecutorService worker;

    public StatsdMetricsPublisher(StatsdMetricsConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(StatsdMetricsPublisher.class);

        check.notEmpty(cfg.getHost(), "host");
        check.positive(cfg.getPort(), "port");
        check.positive(cfg.getMaxQueueSize(), "maximum queue size");
        check.positive(cfg.getBatchSize(), "batch size");

        statsdHost = cfg.getHost();
        statsdPort = cfg.getPort();
        maxQueueSize = cfg.getMaxQueueSize();
        batchSize = cfg.getBatchSize();
        filter = cfg.getFilter();

        queue = new ArrayBlockingQueue<>(maxQueueSize);
    }

    public void start(String nodeHost, int nodePort) throws UnknownHostException {
        assert nodeHost != null : "Node host address us null.";

        log.info("Starting StatsD metrics publisher [{}]", ToString.formatProperties(this));

        InetSocketAddress statsdAddr = new InetSocketAddress(statsdHost, statsdPort);

        if (statsdAddr.getAddress() == null) {
            throw new UnknownHostException(statsdHost);
        }

        String prefix = toSafeHost(nodeHost) + "__" + nodePort;

        synchronized (mux) {
            worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("StatsdMetrics"));

            worker.execute(() -> {
                DatagramChannel udp = null;

                try {
                    boolean throttleErrors = false;

                    while (true) {
                        QueueEntry entry = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

                        if (entry == STOP_ENTRY) {
                            break;
                        }

                        long timestamp = entry.timestamp();
                        int size = entry.metrics().length;

                        if (DEBUG) {
                            log.debug("Publishing metrics [timestamp={}, metrics-size={}]", timestamp, size);
                        }

                        try {
                            if (udp == null || !udp.isConnected()) {
                                close(udp);

                                udp = DatagramChannel.open();

                                udp.connect(statsdAddr);
                            }

                            List<ByteBuffer> packets = encode(prefix, entry.metrics());

                            if (packets != null && !packets.isEmpty()) {
                                doWrite(udp, packets);

                                throttleErrors = false;
                            }

                            if (DEBUG) {
                                log.debug("Published metrics [timestamp={}, metrics-size={}]", timestamp, size);
                            }
                        } catch (IOException e) {
                            close(udp);

                            udp = null;

                            if (throttleErrors) {
                                if (DEBUG) {
                                    log.debug("Throttled error during metrics publishing.", e);
                                }
                            } else {
                                throttleErrors = true;

                                if (log.isWarnEnabled()) {
                                    log.warn("Got an error while publishing metrics to StatsD (will ignore subsequent errors).", e);
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    // No-op.
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected runtime error while publishing metrics to StatsD.", e);

                    synchronized (mux) {
                        if (worker != null) {
                            worker.shutdown();

                            worker = null;
                        }
                    }
                } finally {
                    close(udp);

                    log.info("Stopped StatsD metrics publisher.");
                }
            });
        }
    }

    public void stop() {
        stopAsync().awaitUninterruptedly();
    }

    public boolean isStopped() {
        synchronized (mux) {
            return worker == null;
        }
    }

    public boolean publish(Collection<Metric> metrics) {
        if (metrics != null && !metrics.isEmpty()) {
            synchronized (mux) {
                if (worker == null) {
                    if (DEBUG) {
                        log.debug("Skipped asynchronous metrics publishing since publisher is stopped.");
                    }
                } else {
                    QueueEntry entry = new QueueEntry(System.currentTimeMillis(), metrics.toArray(new Metric[metrics.size()]));

                    if (queue.offer(entry)) {
                        if (DEBUG) {
                            log.debug("Scheduled asynchronous metrics publishing [metrics-size={}]", metrics.size());
                        }

                        return true;
                    } else {
                        if (DEBUG) {
                            log.debug("Skipped asynchronous metrics publishing due to queue overflow [max-queue-size={}]", maxQueueSize);
                        }
                    }
                }
            }
        }

        return false;
    }

    // Package level for testing purposes.
    static String toSafeName(String str) {
        return safeName(UNSAFE_CHARACTERS, str);
    }

    static String toSafeHost(String str) {
        return safeName(UNSAFE_HOST_CHARACTERS, str);
    }

    // Package level for testing purposes.
    int queueSize() {
        return queue.size();
    }

    // Package level for testing purposes.
    void doWrite(DatagramChannel udp, List<ByteBuffer> packets) throws IOException {
        for (ByteBuffer packet : packets) {
            udp.write(packet);
        }
    }

    // Package level for testing purposes.
    Waiting stopAsync() {
        Waiting termination = Waiting.NO_WAIT;

        synchronized (mux) {
            if (worker != null) {
                log.info("Stopping StatsD metrics publisher...");

                while (!queue.offer(STOP_ENTRY)) {
                    queue.clear();
                }

                termination = AsyncUtils.shutdown(worker);

                worker = null;
            }
        }

        return termination;
    }

    private List<ByteBuffer> encode(String prefix, Metric[] metrics) {
        List<ByteBuffer> packets = null;
        StringBuilder buf = null;

        for (int i = 0, packetIdx = 0; i < metrics.length; i++) {
            Metric metric = metrics[i];

            if (filter != null && !filter.accept(metric)) {
                continue;
            }

            if (packets == null) {
                packets = new ArrayList<>(metrics.length / batchSize + 1);

                buf = new StringBuilder();
            }

            if (buf.length() > 0) {
                buf.append('\n');
            }

            buf.append(prefix);
            buf.append('.');
            buf.append(toSafeName(metric.name()));
            buf.append(':');
            buf.append(Long.toString(metric.value()));
            buf.append("|g");

            packetIdx++;

            if (packetIdx == batchSize) {
                packets.add(toBytes(buf));

                buf.setLength(0);
                packetIdx = 0;
            }
        }

        if (buf != null && buf.length() > 0) {
            packets.add(toBytes(buf));
        }

        return packets;
    }

    private ByteBuffer toBytes(StringBuilder buf) {
        byte[] bytes = buf.toString().getBytes(Utils.UTF_8);

        return ByteBuffer.wrap(bytes);
    }

    private void close(DatagramChannel udp) {
        if (udp != null) {
            try {
                udp.close();
            } catch (IOException e) {
                // No-op.
            }
        }
    }

    private static String safeName(Pattern regex, String str) {
        return regex.matcher(str.trim()).replaceAll("_");
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
