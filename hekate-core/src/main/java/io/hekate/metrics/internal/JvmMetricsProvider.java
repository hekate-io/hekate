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

package io.hekate.metrics.internal;

import io.hekate.metrics.MetricConfigBase;
import io.hekate.metrics.MetricsConfigProvider;
import io.hekate.metrics.Probe;
import io.hekate.metrics.ProbeConfig;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.List;

class JvmMetricsProvider implements MetricsConfigProvider {
    private static final int MB = 1024 * 1024;

    private final MemoryMXBean mem = ManagementFactory.getMemoryMXBean();

    @Override
    public List<MetricConfigBase<?>> getMetricsConfig() {
        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();

        return Arrays.asList(
            probe("jvm.mem.used", () -> getUsedMem() / MB),
            probe("jvm.mem.free", () -> getFreeMem() / MB),
            probe("jvm.mem.committed", () -> getCommittedMem() / MB),
            probe("jvm.mem.max", () -> getMaxMem() / MB),
            probe("jvm.mem.nonheap.committed", () -> getNonHeapCommittedMem() / MB),
            probe("jvm.mem.nonheap.used", () -> getNonHeapUsedMem() / MB),
            probe("jvm.mem.heap.committed", () -> getHeapCommittedMem() / MB),
            probe("jvm.mem.heap.used", () -> getHeapUsedMem() / MB),
            probe("jvm.threads.live", threads::getThreadCount),
            probe("jvm.threads.daemon", threads::getDaemonThreadCount),
            probe("jvm.cpu.count", () -> Runtime.getRuntime().availableProcessors()),
            probe("jvm.cpu.load", () -> {
                double avg = os.getSystemLoadAverage();

                if (avg < 0) {
                    return -1;
                } else {
                    return (long)(avg * 100);
                }
            })
        );
    }

    private long getUsedMem() {
        return getHeapUsedMem() + getNonHeapUsedMem();
    }

    private long getCommittedMem() {
        return getHeapCommittedMem() + getNonHeapCommittedMem();
    }

    private long getFreeMem() {
        return Runtime.getRuntime().freeMemory();
    }

    private long getMaxMem() {
        return Runtime.getRuntime().maxMemory();
    }

    private long getHeapUsedMem() {
        return mem.getHeapMemoryUsage().getUsed();
    }

    private long getHeapCommittedMem() {
        return mem.getHeapMemoryUsage().getCommitted();
    }

    private long getNonHeapUsedMem() {
        return mem.getNonHeapMemoryUsage().getUsed();
    }

    private long getNonHeapCommittedMem() {
        return mem.getNonHeapMemoryUsage().getCommitted();
    }

    private ProbeConfig probe(String name, Probe probe) {
        return new ProbeConfig(name).withProbe(probe);
    }
}
