/*
 * Copyright 2017 The Hekate Project
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
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.unicast.LoadBalancers;
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
    public enum Mode {
        SOCKET_1_NIO_2_WORKER_2(1, 2, 4),
        SOCKET_1_NIO_4_WORKER_8(1, 4, 8),
        SOCKET_1_NIO_4_WORKER_0(1, 4, 0);

        private final int socket;

        private final int nio;

        private final int workers;

        Mode(int socket, int nio, int workers) {
            this.socket = socket;
            this.nio = nio;
            this.workers = workers;
        }
    }

    public static class BenchmarkContext extends MultiNodeBenchmarkContext {
        @Param({
            "SOCKET_1_NIO_2_WORKER_2",
            "SOCKET_1_NIO_4_WORKER_8",
            "SOCKET_1_NIO_4_WORKER_0"}
        )
        private Mode mode;

        private MessagingChannel<byte[]> channel;

        public BenchmarkContext() {
            super(3);
        }

        @Override
        protected void configure(int index, HekateBootstrap boot) {
            int sockets = mode.socket;
            int nioThreadPoolSize = mode.nio;
            int workerThreadPoolSize = mode.workers;

            MessagingChannelConfig<byte[]> channel = new MessagingChannelConfig<byte[]>()
                .withName("test_channel")
                .withSockets(sockets)
                .withNioThreads(nioThreadPoolSize)
                .withWorkerThreads(workerThreadPoolSize)
                .withLoadBalancer(LoadBalancers.toRandom());

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
        }

        @Override
        protected void initialize(List<Hekate> nodes) throws Exception {
            channel = nodes.get(0).get(MessagingService.class).<byte[]>get("test_channel").forRemotes();
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
    public void measure(BenchmarkContext ctx) throws Exception {
        ctx.channel.send(randomBytes()).get();
    }

    private static byte[] randomBytes() {
        byte[] b = new byte[100];

        ThreadLocalRandom.current().nextBytes(b);

        return b;
    }
}
