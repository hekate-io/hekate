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

package io.hekate.javadoc.cluster.event;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.core.Hekate;
import org.junit.Test;

public class ClusterEventListenerJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleListener() throws Exception {
        // Start:cluster_event_listener
        class ExampleListener implements ClusterEventListener {
            @Override
            public void onEvent(ClusterEvent event) {
                switch (event.type()) {
                    case JOIN: {
                        ClusterJoinEvent join = event.asJoin();

                        System.out.println("Joined : " + join.topology());

                        break;
                    }
                    case CHANGE: {
                        ClusterChangeEvent change = event.asChange();

                        System.out.println("Topology change :" + change.topology());
                        System.out.println("    Added nodes :" + change.added());
                        System.out.println("  Removed nodes :" + change.removed());

                        break;
                    }
                    case LEAVE: {
                        ClusterLeaveEvent leave = event.asLeave();

                        System.out.println("Left : " + leave.topology());

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unsupported event type: " + event);
                    }
                }
            }
        }
        // End:cluster_event_listener

        Hekate node = createNode();

        node.cluster().addListener(new ExampleListener());

        node.join();
    }
}
