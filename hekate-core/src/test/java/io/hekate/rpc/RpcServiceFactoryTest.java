package io.hekate.rpc;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.util.List;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class RpcServiceFactoryTest extends HekateTestBase {
    private final RpcServiceFactory factory = new RpcServiceFactory();

    @Test
    public void testClients() {
        assertNull(factory.getClients());

        RpcClientConfig cl1 = new RpcClientConfig();
        RpcClientConfig cl2 = new RpcClientConfig();

        List<RpcClientConfig> clients = asList(cl1, cl2);

        factory.setClients(clients);

        assertEquals(clients, factory.getClients());

        factory.setClients(null);

        assertNull(factory.getClients());

        assertSame(factory, factory.withClient(cl1));
        assertEquals(singletonList(cl1), factory.getClients());
    }

    @Test
    public void testClientProviders() {
        assertNull(factory.getClientProviders());

        RpcClientConfigProvider p1 = () -> null;
        RpcClientConfigProvider p2 = () -> null;

        factory.setClientProviders(asList(p1, p2));

        assertEquals(asList(p1, p2), factory.getClientProviders());

        factory.setClientProviders(null);

        assertNull(factory.getClientProviders());

        assertSame(factory, factory.withClientProvider(p1));
        assertEquals(singletonList(p1), factory.getClientProviders());
    }

    @Test
    public void testServers() {
        assertNull(factory.getServers());

        RpcServerConfig cl1 = new RpcServerConfig();
        RpcServerConfig cl2 = new RpcServerConfig();

        List<RpcServerConfig> servers = asList(cl1, cl2);

        factory.setServers(servers);

        assertEquals(servers, factory.getServers());

        factory.setServers(null);

        assertNull(factory.getServers());

        assertSame(factory, factory.withServer(cl1));
        assertEquals(singletonList(cl1), factory.getServers());
    }

    @Test
    public void testServerProviders() {
        assertNull(factory.getServerProviders());

        RpcServerConfigProvider p1 = () -> null;
        RpcServerConfigProvider p2 = () -> null;

        factory.setServerProviders(asList(p1, p2));

        assertEquals(asList(p1, p2), factory.getServerProviders());

        factory.setServerProviders(null);

        assertNull(factory.getServerProviders());

        assertSame(factory, factory.withServerProvider(p1));
        assertEquals(singletonList(p1), factory.getServerProviders());
    }

    @Test
    public void testWorkerThreads() {
        assertEquals(Runtime.getRuntime().availableProcessors(), factory.getWorkerThreads());

        factory.setWorkerThreads(10);

        assertEquals(10, factory.getWorkerThreads());

        assertSame(factory, factory.withWorkerThreads(100));
        assertEquals(100, factory.getWorkerThreads());
    }

    @Test
    public void testNioThreads() {
        assertEquals(0, factory.getNioThreads());

        factory.setNioThreads(10);

        assertEquals(10, factory.getNioThreads());

        assertSame(factory, factory.withNioThreads(100));
        assertEquals(100, factory.getNioThreads());
    }

    @Test
    public void testIdleSocketTimeout() {
        assertEquals(0, factory.getIdleSocketTimeout());

        factory.setIdleSocketTimeout(10);

        assertEquals(10, factory.getIdleSocketTimeout());

        assertSame(factory, factory.withIdleSocketTimeout(100));
        assertEquals(100, factory.getIdleSocketTimeout());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(factory), factory.toString());
    }
}
