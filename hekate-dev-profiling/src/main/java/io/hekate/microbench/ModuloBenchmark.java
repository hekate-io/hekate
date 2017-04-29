package io.hekate.microbench;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

public class ModuloBenchmark {
    public static void main(String[] args) throws IOException, RunnerException {
        Options opt = new OptionsBuilder()
            .include(ModuloBenchmark.class.getName())
            .forks(1)
            .threads(4)
            .warmupIterations(10)
            .build();

        new Runner(opt).run();
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int modulo() {
        return ThreadLocalRandom.current().nextInt() % 1011;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public int pow() {
        return ThreadLocalRandom.current().nextInt() & 1024;
    }
}
