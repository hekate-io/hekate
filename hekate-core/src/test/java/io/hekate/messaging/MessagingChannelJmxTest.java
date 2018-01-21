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

package io.hekate.messaging;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static io.hekate.core.jmx.JmxTestUtils.verifyJmxTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MessagingChannelJmxTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(MessagingServiceFactory.class, messaging -> {
                messaging.withChannel(MessagingChannelConfig.of(MessagingChannelJmxTest.class)
                    .withName("test.channel")
                    .withIdleSocketTimeout(100500)
                    .withNioThreads(2)
                    .withWorkerThreads(3)
                    .withPartitions(2048)
                    .withBackupNodes(4)
                    .withLogCategory("io.hekate.test.channel")
                    .withBackPressure(new MessagingBackPressureConfig()
                        .withOutOverflowPolicy(MessagingOverflowPolicy.FAIL)
                        .withOutLowWatermark(1001)
                        .withOutHighWatermark(1002)
                        .withInLowWatermark(1003)
                        .withInHighWatermark(1004)
                    )
                    .withReceiver(msg -> {
                        // No-op.
                    })
                );
            });
        }).join();

        MessagingChannel<Object> channel = node.messaging().channel("test.channel");

        ObjectName name = node.get(JmxService.class).nameFor(MessagingChannelJmx.class, channel.name());

        assertEquals(channel.id().toString(), jmxAttribute(name, "Id", node));
        assertEquals(channel.name(), jmxAttribute(name, "Name", node));
        assertEquals(MessagingChannelJmxTest.class.getName(), jmxAttribute(name, "BaseType", node));
        assertEquals(2, (int)jmxAttribute(name, "NioThreads", node));
        assertEquals(3, (int)jmxAttribute(name, "WorkerThreads", node));
        assertEquals(2048, (int)jmxAttribute(name, "Partitions", node));
        assertEquals(4, (int)jmxAttribute(name, "BackupNodes", node));
        assertEquals(100500, (long)jmxAttribute(name, "IdleSocketTimeout", node));
        assertTrue(jmxAttribute(name, "Receiver", node));
        assertEquals("io.hekate.test.channel", jmxAttribute(name, "LogCategory", node));
        assertEquals(MessagingOverflowPolicy.FAIL.name(), jmxAttribute(name, "BackPressureOutOverflowPolicy", node));
        assertEquals(1001, (int)jmxAttribute(name, "BackPressureOutLowWatermark", node));
        assertEquals(1002, (int)jmxAttribute(name, "BackPressureOutHighWatermark", node));
        assertEquals(1003, (int)jmxAttribute(name, "BackPressureInLowWatermark", node));
        assertEquals(1004, (int)jmxAttribute(name, "BackPressureInHighWatermark", node));

        verifyJmxTopology(channel.cluster().topology(), jmxAttribute(name, "Topology", node));
    }
}
