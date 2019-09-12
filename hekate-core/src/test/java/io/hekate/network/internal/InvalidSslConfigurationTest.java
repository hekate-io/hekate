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

package io.hekate.network.internal;

import io.hekate.HekateTestContext;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkSslConfig;
import io.hekate.test.NetworkClientCallbackMock;
import java.net.ConnectException;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLHandshakeException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class InvalidSslConfigurationTest extends NetworkTestBase {
    public InvalidSslConfigurationTest(HekateTestContext ctx) {
        super(ctx);
    }

    @Before
    public void checkSslEnabled() {
        assumeTrue(context().ssl().isPresent());
    }

    @Test
    public void testInvalidCertificate() throws Exception {
        NetworkServer server = createServer();

        get(server.start(newServerAddress()));

        NetworkSslConfig ssl = new NetworkSslConfig()
            .withProvider(context().ssl().get().getProvider())
            .withKeyStorePath("ssl/hekate-test2.jks")
            .withKeyStorePassword("hekate-test2")
            .withTrustStorePath("ssl/hekate-test2.jks")
            .withTrustStorePassword("hekate-test2");

        NetworkClient<String> client = createClient(cfg ->
            cfg.setSsl(NettySslUtils.clientContext(ssl, context().resources()))
        );

        try {
            client.connect(server.address(), new NetworkClientCallbackMock<>()).get();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(getStacktrace(e), ErrorUtils.isCausedBy(SSLHandshakeException.class, e));
        }
    }

    @Test
    public void testSslNotConfiguredOnClient() throws Exception {
        NetworkServer server = createServer();

        get(server.start(newServerAddress()));

        NetworkClient<String> client = createClient(cfg ->
            cfg.setSsl(null)
        );

        try {
            client.connect(server.address(), new NetworkClientCallbackMock<>()).get();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(getStacktrace(e), ErrorUtils.isCausedBy(ConnectException.class, e));
        }
    }

    @Test
    public void testSslNotConfiguredOnServer() throws Exception {
        NetworkServer server = createAndConfigureServer(cfg ->
            cfg.setSsl(null)
        );

        get(server.start(newServerAddress()));

        try {
            createClient().connect(server.address(), new NetworkClientCallbackMock<>()).get();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(getStacktrace(e), ErrorUtils.isCausedBy(ConnectException.class, e));
        }
    }
}
