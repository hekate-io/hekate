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
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockServiceFactory;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static java.util.stream.Collectors.toList;

public class LocksBenchmark {
    public static class BenchmarkContext extends MultiNodeBenchmarkContext {
        private List<LockRegion> regions;

        public BenchmarkContext() {
            super(3);
        }

        @Override
        protected void configure(int index, HekateBootstrap boot) {
            boot.withService(new LockServiceFactory()
                .withRegion(new LockRegionConfig("test"))
            );
        }

        @Override
        protected void initialize(List<Hekate> nodes) {
            regions = nodes.stream()
                .map(n -> n.locks().region("test"))
                .collect(toList());
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
            .include(Thread.currentThread().getStackTrace()[1].getClassName())
            .forks(1)
            .threads(20)
            .warmupIterations(10)
            .measurementIterations(20)
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void measure(BenchmarkContext ctx) {
        int regionIdx = ThreadLocalRandom.current().nextInt(ctx.regions.size());

        DistributedLock lock = ctx.regions.get(regionIdx).get(UUID.randomUUID().toString());

        lock.lock();
        lock.unlock();
    }
}
