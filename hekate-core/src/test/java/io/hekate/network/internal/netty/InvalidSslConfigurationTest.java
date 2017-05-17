package io.hekate.network.internal.netty;

import io.hekate.HekateTestContext;
import io.hekate.core.internal.util.Utils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkSslConfig;
import io.hekate.network.internal.NetworkClientCallbackMock;
import io.hekate.network.internal.NetworkServer;
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
            cfg.setSsl(NettySslUtil.clientContext(ssl, context().resources()))
        );

        try {
            client.connect(server.address(), new NetworkClientCallbackMock<>()).get();

            fail("Error was expected.");
        } catch (ExecutionException e) {
            assertTrue(getStacktrace(e), Utils.isCausedBy(e, SSLHandshakeException.class));
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
            assertTrue(getStacktrace(e), Utils.isCausedBy(e, ConnectException.class));
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
            assertTrue(getStacktrace(e), Utils.isCausedBy(e, ConnectException.class));
        }
    }
}
