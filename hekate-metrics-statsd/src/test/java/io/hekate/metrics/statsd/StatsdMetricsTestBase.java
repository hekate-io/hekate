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

import io.hekate.HekateNodeTestBase;
import io.hekate.metrics.Metric;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;

import static org.junit.Assert.assertNotNull;

public class StatsdMetricsTestBase extends HekateNodeTestBase {
    interface IoTask {
        void run() throws IOException;
    }

    static class TestMetric implements Metric {
        private final String name;

        private final long value;

        public TestMetric(String name, long value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public long value() {
            return value;
        }
    }

    protected final int testPort = newTcpPort();

    private final BlockingQueue<String> received = new LinkedBlockingDeque<>();

    private DatagramSocket udp;

    private Future<Void> udpFuture;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        udp = new DatagramSocket(testPort);

        udpFuture = runAsync(() -> {
            try {
                byte[] bytes = new byte[512];

                DatagramPacket packet = new DatagramPacket(bytes, 512);

                while (!udp.isClosed()) {
                    udp.receive(packet);

                    received.add(new String(packet.getData(), "utf-8").trim());

                    Arrays.fill(bytes, (byte)0);
                }
            } catch (IOException e) {
                // No-op.
            }

            return null;
        });
    }

    @After
    @Override
    public void tearDown() throws Exception {
        udp.close();

        get(udpFuture);

        super.tearDown();
    }

    protected String receiveNext() throws Exception {
        String data = received.poll(3, TimeUnit.SECONDS);

        assertNotNull("Failed to await for data from UDP socket.", data);

        say("Received: " + data);

        return data;
    }
}
