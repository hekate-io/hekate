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

package io.hekate.task.internal;

import io.hekate.HekateNodeContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.Utils;
import io.hekate.task.TaskServiceFactory;
import io.hekate.util.HekateFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Before;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class TaskServiceTestBase extends HekateNodeContextTestBase {
    protected static class NonSerializable {
        // No-op.
    }

    public static class NonSerializableTestException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private final NonSerializable nonSerializable = new NonSerializable();

        @Override
        public String toString() {
            return super.toString() + nonSerializable;
        }
    }

    protected static final AtomicInteger COUNTER = new AtomicInteger();

    protected static final List<ClusterNode> NODES = Collections.synchronizedList(new ArrayList<>());

    protected static final AtomicReference<HekateTestNode> REF = new AtomicReference<>();

    public TaskServiceTestBase(HekateTestContext params) {
        super(params);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        COUNTER.set(0);
        NODES.clear();
        REF.set(null);
    }

    protected void assertErrorCausedBy(HekateFuture future, Class<? extends Throwable> type) throws Exception {
        assertErrorCausedBy(future, type, null);
    }

    protected void assertErrorCausedBy(HekateFuture<?, ?> future, Class<? extends Throwable> type, Consumer<Throwable> check)
        throws Exception {
        doAssertErrorCausedBy(type, check, () -> get(future));
        doAssertErrorCausedBy(type, check, future::get);
        doAssertErrorCausedBy(type, check, future::getUninterruptedly);
    }

    protected List<HekateTestNode> createAndJoin(int size) throws Exception {
        List<HekateTestNode> nodes = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            nodes.add(createTaskNode().join());
        }

        awaitForTopology(nodes);

        return nodes;
    }

    protected HekateTestNode createTaskNode() throws Exception {
        return createTaskNode(null);
    }

    protected HekateTestNode createTaskNode(NodeConfigurer configurer) throws Exception {
        return createNode(c -> {
            c.withService(new TaskServiceFactory());

            if (configurer != null) {
                configurer.configure(c);
            }
        });
    }

    private void doAssertErrorCausedBy(Class<? extends Throwable> type, Consumer<Throwable> check, Callable<?> t) throws Exception {
        try {
            t.call();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(getStacktrace(e.getCause()), Utils.isCausedBy(e.getCause(), type));

            if (check != null) {
                check.accept(Utils.findCause(e.getCause(), type));
            }
        }
    }
}
