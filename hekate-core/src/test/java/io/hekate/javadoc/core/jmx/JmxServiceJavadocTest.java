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

package io.hekate.javadoc.core.jmx;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import java.lang.management.ManagementFactory;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class JmxServiceJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleService() throws Exception {
        // Start:configure
        // Prepare service factory and configure some options.
        JmxServiceFactory factory = new JmxServiceFactory();

        // (optional) explicitly specify MBean server.
        factory.setServer(ManagementFactory.getPlatformMBeanServer());

        // ...other options...

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure

        // Start:access
        JmxService jmx = hekate.get(JmxService.class);
        // End:access

        assertNotNull(jmx);

        hekate.leave();
    }
}
