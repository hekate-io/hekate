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

package io.hekate.microbench;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class MessagingBenchmark {
    @SuppressWarnings("unused")
    public enum Mode {
        NIO_1_WORKER_1(1, 1),
        NIO_1_WORKER_0(1, 0),
        NIO_2_WORKER_0(2, 0),
        NIO_1_WORKER_4(1, 4),
        NIO_2_WORKER_4(2, 4);

        private final int nio;

        private final int workers;

        Mode(int nio, int workers) {
            this.nio = nio;
            this.workers = workers;
        }
    }

    public static class BenchmarkContext extends MultiNodeBenchmarkContext {
        @Param({
            "NIO_1_WORKER_1",
            "NIO_1_WORKER_0",
            "NIO_2_WORKER_0",
            "NIO_1_WORKER_4",
            "NIO_2_WORKER_4"
        })
        private Mode mode;

        private MessagingChannel<byte[]> channel;

        public BenchmarkContext() {
            super(3);
        }

        @Override
        protected void configure(int index, HekateBootstrap boot) {
            int nioThreadPoolSize = mode.nio;
            int workerThreadPoolSize = mode.workers;

            MessagingChannelConfig<byte[]> channel = MessagingChannelConfig.of(byte[].class)
                .withName("test.channel")
                .withNioThreads(nioThreadPoolSize)
                .withWorkerThreads(workerThreadPoolSize);

            if (index > 0) {
                channel.withReceiver(msg -> {
                    if (msg.mustReply()) {
                        msg.reply(randomBytes());
                    }
                });
            }

            boot.withService(new MessagingServiceFactory()
                .withChannel(channel)
            );
            boot.withService(new LocalMetricsServiceFactory());
        }

        @Override
        protected void initialize(List<Hekate> nodes) {
            channel = nodes.get(0).messaging().channel("test.channel", byte[].class).forRemotes();
        }
    }

    public static void main(String[] args) throws IOException, RunnerException {
        Options opt = new OptionsBuilder()
            .include(Thread.currentThread().getStackTrace()[1].getClassName())
            .forks(1)
            .threads(20)
            .warmupIterations(10)
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void measure(BenchmarkContext ctx) throws MessagingFutureException, InterruptedException {
        ctx.channel.request(randomBytes()).get();
    }

    private static byte[] randomBytes() {
        byte[] b = new byte[100];

        ThreadLocalRandom.current().nextBytes(b);

        return b;
    }
}
