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

import io.hekate.codec.JdkCodecFactory;
import io.hekate.codec.fst.FstCodecFactory;
import io.hekate.codec.kryo.KryoCodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.rpc.Rpc;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcServiceFactory;
import java.util.List;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class RpcBenchmark {
    public enum CodecType {
        JDK,
        KRYO,
        FST
    }

    @SuppressWarnings("unused")
    public enum Mode {
        WORKER_0_KRYO(1, 0, CodecType.KRYO),
        WORKER_0_FST(1, 0, CodecType.FST),
        WORKER_0_JDK(1, 0, CodecType.JDK),
        WORKER_4_KRYO(1, 4, CodecType.KRYO),
        WORKER_4_FST(1, 4, CodecType.FST),
        WORKER_4_JDK(1, 4, CodecType.JDK),
        WORKER_8_KRYO(1, 8, CodecType.KRYO),
        WORKER_8_FST(1, 8, CodecType.FST),
        WORKER_8_JDK(1, 8, CodecType.JDK);

        private final int nio;

        private final int workers;

        private final CodecType codecType;

        Mode(int nio, int workers, CodecType codecType) {
            this.nio = nio;
            this.workers = workers;
            this.codecType = codecType;
        }
    }

    @Rpc
    public interface TestRpc {
        Object call(Object arg);
    }

    public static class TestRpcImpl implements TestRpc {
        @Override
        public Object call(Object arg) {
            return arg;
        }
    }

    public static class BenchmarkContext extends MultiNodeBenchmarkContext {
        @Param({
            "WORKER_0_KRYO",
            "WORKER_0_FST",
            "WORKER_0_JDK",
            "WORKER_4_KRYO",
            "WORKER_4_FST",
            "WORKER_4_JDK",
            "WORKER_8_KRYO",
            "WORKER_8_FST",
            "WORKER_8_JDK"
        })
        private Mode mode;

        private TestRpc rpc;

        public BenchmarkContext() {
            super(3);
        }

        @Override
        protected void configure(int index, HekateBootstrap boot) {
            int nioThreadPoolSize = mode.nio;
            int workerThreadPoolSize = mode.workers;

            boot.withService(new RpcServiceFactory()
                .withNioThreads(nioThreadPoolSize)
                .withWorkerThreads(workerThreadPoolSize)
                .withServer(new RpcServerConfig()
                    .withHandler(new TestRpcImpl())
                )
            );

            switch (mode.codecType) {
                case JDK: {
                    boot.setDefaultCodec(new JdkCodecFactory<>());

                    break;
                }
                case KRYO: {
                    boot.setDefaultCodec(new KryoCodecFactory<>());

                    break;
                }
                case FST: {
                    boot.setDefaultCodec(new FstCodecFactory<>());

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected codec type: " + mode.codecType);
                }
            }
        }

        @Override
        protected void initialize(List<Hekate> nodes) {
            rpc = nodes.get(0).rpc().clientFor(TestRpc.class).build();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(Thread.currentThread().getStackTrace()[1].getClassName())
            .forks(1)
            .threads(30)
            .warmupIterations(10)
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    public Object measure(BenchmarkContext ctx) throws InterruptedException {
        TestBean arg = TestBean.random();

        return ctx.rpc.call(arg);
    }
}
