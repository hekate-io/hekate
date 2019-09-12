/*
 * Copyright 2019 The Hekate Project
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
    public String nodeName() {
        return nodeName;
    }
}
