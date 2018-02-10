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

import io.hekate.metrics.Metric;
import io.hekate.util.async.Waiting;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StatsdMetricsPublisherTest extends StatsdMetricsTestBase {
    private interface WriteDelegate {
        void write(DatagramChannel udp, List<ByteBuffer> packets, IoTask original) throws IOException;
    }

    private static final int TEST_BATCH_SIZE = 5;

    private StatsdMetricsPublisher publisher;

    private volatile WriteDelegate writeDelegate;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        StatsdMetricsConfig cfg = new StatsdMetricsConfig()
            .withHost(InetAddress.getLocalHost().getHostAddress())
            .withPort(testPort)
            .withBatchSize(TEST_BATCH_SIZE);

        publisher = new StatsdMetricsPublisher(cfg) {
            @Override
            void doWrite(DatagramChannel udp, List<ByteBuffer> packets) throws IOException {
                if (writeDelegate == null) {
                    super.doWrite(udp, packets);
                } else {
                    writeDelegate.write(udp, packets, () ->
                        super.doWrite(udp, packets)
                    );
                }
            }
        };
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (publisher != null) {
                publisher.stop();
            }
        }
    }

    @Test
    public void testStartStop() throws Exception {
        for (int i = 0; i < 10; i++) {
            String nodeHost = "localhost" + i;
            int nodePort = 10000 + i;

            publisher.start(nodeHost, nodePort);

            publisher.publish(singleton(new TestMetric("testA" + i + ".testB" + i, 10000 + i)));

            assertEquals(nodeHost + "__" + nodePort + ".testA" + i + ".testB" + i + ":1000" + i + "|g", receiveNext());

            publisher.stop();

            assertFalse(publisher.publish(singleton(new TestMetric("no-way-after-stop", i))));
        }
    }

    @Test
    public void testPublishSingle() throws Exception {
        publisher.start("localhost", 9999);

        for (int i = 0; i < 10; i++) {
            publisher.publish(singleton(new TestMetric("testA" + i + ".testB" + i, 10000 + i)));

            assertEquals("localhost__9999.testA" + i + ".testB" + i + ":1000" + i + "|g", receiveNext());
        }

        publisher.stop();
    }

    @Test
    public void testPublishBulk() throws Exception {
        publisher.start("localhost", 9999);

        for (int i = 0; i < 10; i++) {
            publisher.publish(Arrays.asList(
                new TestMetric("testA" + i + ".testA" + i, 10000 + i),
                new TestMetric("testB" + i + ".testB" + i, 10000 + i),
                new TestMetric("testC" + i + ".testC" + i, 10000 + i),
                new TestMetric("testD" + i + ".testD" + i, 10000 + i)
            ));

            String expected = ""
                + "localhost__9999.testA" + i + ".testA" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testB" + i + ".testB" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testC" + i + ".testC" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testD" + i + ".testD" + i + ":1000" + i + "|g";

            assertEquals(expected, receiveNext());
        }

        publisher.stop();
    }

    @Test
    public void testPublishBulkMultipleBatches() throws Exception {
        assertEquals(5, TEST_BATCH_SIZE);

        publisher.start("localhost", 9999);

        for (int i = 0; i < 10; i++) {
            List<Metric> metrics = new ArrayList<>();

            publisher.publish(Arrays.asList(
                new TestMetric("testA" + i + ".testA" + i, 10000 + i),
                new TestMetric("testB" + i + ".testB" + i, 10000 + i),
                new TestMetric("testC" + i + ".testC" + i, 10000 + i),
                new TestMetric("testD" + i + ".testD" + i, 10000 + i),
                new TestMetric("testE" + i + ".testE" + i, 10000 + i),
                new TestMetric("testF" + i + ".testF" + i, 10000 + i),
                new TestMetric("testG" + i + ".testG" + i, 10000 + i),
                new TestMetric("testH" + i + ".testH" + i, 10000 + i),
                new TestMetric("testI" + i + ".testI" + i, 10000 + i),
                new TestMetric("testJ" + i + ".testJ" + i, 10000 + i),
                new TestMetric("testK" + i + ".testK" + i, 10000 + i)
            ));

            publisher.publish(metrics);

            List<String> received = new ArrayList<>();

            received.add(receiveNext());
            received.add(receiveNext());
            received.add(receiveNext());

            Collections.sort(received);

            String expected = ""
                + "localhost__9999.testA" + i + ".testA" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testB" + i + ".testB" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testC" + i + ".testC" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testD" + i + ".testD" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testE" + i + ".testE" + i + ":1000" + i + "|g";

            assertEquals(expected, received.get(0));

            expected = ""
                + "localhost__9999.testF" + i + ".testF" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testG" + i + ".testG" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testH" + i + ".testH" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testI" + i + ".testI" + i + ":1000" + i + "|g\n"
                + "localhost__9999.testJ" + i + ".testJ" + i + ":1000" + i + "|g";

            assertEquals(expected, received.get(1));

            expected = "localhost__9999.testK" + i + ".testK" + i + ":1000" + i + "|g";

            assertEquals(expected, received.get(2));
        }

        publisher.stop();
    }

    @Test
    public void testQueue() throws Exception {
        int maxQueueSize = StatsdMetricsConfig.DEFAULT_QUEUE_SIZE;

        CountDownLatch resume = new CountDownLatch(1);

        writeDelegate = (db, points, original) -> {
            await(resume);

            original.run();
        };

        publisher.start("test-host", 9999);

        for (int i = 0; i < maxQueueSize * 2; i++) {
            publisher.publish(singleton(new TestMetric("test.metric" + i, i)));
        }

        resume.countDown();

        busyWait("empty queue", () ->
            publisher.queueSize() == 0
        );

        publisher.stop();

        for (int i = 0; i < maxQueueSize; i++) {
            assertEquals("test_host__9999.test.metric" + i + ":" + i + "|g", receiveNext());
        }
    }

    @Test
    public void testStopWithFullQueue() throws Exception {
        CountDownLatch resume = new CountDownLatch(1);

        try {
            writeDelegate = (db, points, original) -> {
                try {
                    resume.await();
                } catch (InterruptedException e) {
                    fail("Thread was unexpectedly interrupted.");
                }

                original.run();
            };

            publisher.start("test-host", 10002);

            int maxQueueSize = StatsdMetricsConfig.DEFAULT_QUEUE_SIZE;

            for (int i = 0; i < maxQueueSize * 2; i++) {
                publisher.publish(singleton(new TestMetric("test.metric", i)));
            }

            int queueSize = publisher.queueSize();

            assertTrue(queueSize > 0 && queueSize <= maxQueueSize);
        } finally {
            Waiting stopped = publisher.stopAsync();

            resume.countDown();

            stopped.await();
        }
    }

    @Test
    public void testStopOnError() throws Exception {
        writeDelegate = (udp, packets, original) -> {
            throw TEST_ERROR;
        };

        publisher.start("test-host", 10002);

        publisher.publish(singleton(new TestMetric("test.metric", 10)));

        busyWait("publisher stop", () -> publisher.isStopped());
    }

    @Test
    public void testErrorRecover() throws Exception {
        int attempts = 10;
        int failures = 5;

        CountDownLatch ready = new CountDownLatch(attempts);

        writeDelegate = (udp, packets, original) -> {
            try {
                if (ready.getCount() < failures) {
                    udp.close();
                }

                original.run();
            } finally {
                ready.countDown();
            }
        };

        publisher.start("test-host", 9999);

        for (int i = 0; i < attempts; i++) {
            publisher.publish(singleton(new TestMetric("test" + i, i)));
        }

        await(ready);

        for (int i = 0; i <= attempts - failures; i++) {
            assertEquals("test_host__9999.test" + i + ":" + i + "|g", receiveNext());
        }
    }
}
