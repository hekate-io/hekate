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

package io.hekate.javadoc.task;

import foo.bar.SomeBean;
import io.hekate.core.Hekate;
import io.hekate.inject.HekateInject;
import io.hekate.messaging.MessagingService;
import io.hekate.task.RunnableTask;
import org.springframework.beans.factory.annotation.Autowired;

// Start:task
@HekateInject // <-- Enables injection on instances of this class.
public class ExampleTask implements RunnableTask {
    private static final long serialVersionUID = 1;

    @Autowired
    // Remote node that executes this task.
    private Hekate remoteNode;

    @Autowired
    // Service of a remote Hekate node.
    private MessagingService messaging;

    @Autowired
    // Some other bean from the Spring context of a remote node.
    private SomeBean someBean;

    @Override
    public void run() {
        assert remoteNode != null;
        assert messaging != null;
        assert someBean != null;

        // ... do some work with injected components...
    }
}
// End:task

