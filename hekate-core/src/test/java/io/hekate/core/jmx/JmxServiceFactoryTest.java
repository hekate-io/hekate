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

package io.hekate.core.jmx;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import javax.management.MBeanServer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class JmxServiceFactoryTest extends HekateTestBase {
    private final JmxServiceFactory factory = new JmxServiceFactory();

    @Test
    public void testDomain() {
        assertEquals(JmxServiceFactory.DEFAULT_DOMAIN, factory.getDomain());

        factory.setDomain(null);

        assertNull(factory.getDomain());

        assertSame(factory, factory.withDomain("some.domain"));

        assertEquals("some.domain", factory.getDomain());
    }

    @Test
    public void testServer() {
        assertNotNull(factory.getServer());

        MBeanServer initServer = factory.getServer();
        MBeanServer newServer = mock(MBeanServer.class);

        factory.setServer(newServer);

        assertSame(newServer, factory.getServer());

        factory.setServer(null);

        assertSame(initServer, factory.getServer());

        assertSame(factory, factory.withServer(newServer));

        assertSame(newServer, factory.getServer());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(factory), factory.toString());
    }
}
