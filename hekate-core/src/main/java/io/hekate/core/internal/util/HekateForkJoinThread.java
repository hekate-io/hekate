package io.hekate.core.internal.util;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;

class HekateForkJoinThread extends ForkJoinWorkerThread implements HekateNodeNameAwareThread {
    private final String nodeName;

    public HekateForkJoinThread(String nodeName, String threadName, ForkJoinPool pool) {
        super(pool);

        this.nodeName = nodeName;

        setName(HekateThread.makeName(nodeName, threadName));
    }

    @Override
    public String getNodeName() {
        return nodeName;
    }
}
