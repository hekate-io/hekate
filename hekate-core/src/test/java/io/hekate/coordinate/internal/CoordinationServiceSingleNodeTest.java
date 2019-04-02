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

package io.hekate.coordinate.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationProcess;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationServiceFactory;
import io.hekate.coordinate.CoordinatorContext;
import io.hekate.core.internal.HekateTestNode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class CoordinationServiceSingleNodeTest extends HekateNodeTestBase {
    @Test
    public void testEmptyProcesses() throws Exception {
        HekateTestNode node = createNode(boot ->
            boot.withService(new CoordinationServiceFactory())
        ).join();

        assertTrue(node.coordination().allProcesses().isEmpty());

        assertFalse(node.coordination().hasProcess("no-such-process"));

        expect(IllegalArgumentException.class, () -> node.coordination().process("no-such-process"));
    }

    @Test
    public void testMultipleProcesses() throws Exception {
        CoordinationHandler handler = mock(CoordinationHandler.class);

        doAnswer(invocation -> {
            ((CoordinatorContext)invocation.getArguments()[0]).complete();

            return null;
        }).when(handler).coordinate(any());

        HekateTestNode node = createNode(boot ->
            boot.withService(new CoordinationServiceFactory()
                .withProcess(new CoordinationProcessConfig("process1").withHandler(handler))
                .withProcess(new CoordinationProcessConfig("process2").withHandler(handler))
            )
        ).join();

        assertTrue(node.coordination().hasProcess("process1"));
        assertTrue(node.coordination().hasProcess("process2"));

        CoordinationProcess process1 = node.coordination().process("process1");
        CoordinationProcess process2 = node.coordination().process("process1");

        assertNotNull(process1);
        assertNotNull(process2);

        assertEquals(2, node.coordination().allProcesses().size());
        assertTrue(node.coordination().allProcesses().contains(process1));
        assertTrue(node.coordination().allProcesses().contains(process1));

        get(process1.future());
        get(process2.future());
    }
}
