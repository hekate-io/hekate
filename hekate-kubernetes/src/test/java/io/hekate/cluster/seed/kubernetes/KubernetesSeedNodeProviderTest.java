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

package io.hekate.cluster.seed.kubernetes;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import io.hekate.HekateTestBase;
import io.hekate.core.HekateException;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KubernetesSeedNodeProviderTest extends HekateTestBase {
    public static final String CLUSTER = "ignore";

    private final KubernetesServer server = new KubernetesServer(true, true);

    @Before
    public void setUp() {
        server.before();
    }

    @After
    public void tearDown() {
        server.after();
    }

    @Test
    public void testSuccess() throws Exception {
        String portName1 = KubernetesSeedNodeProviderConfig.DEFAULT_CONTAINER_PORT_NAME;
        String portName2 = "some-port-name";

        KubernetesSeedNodeProvider provider1 = createProvider(portName1);

        assertTrue(provider1.findSeedNodes(CLUSTER).isEmpty());

        // Pods with matching port name.
        for (int i = 0; i < 3; i++) {
            createPod(portName1, "pod" + i, "127.0.0.1", 10000 + i, i > 0);
        }

        // Pods with non-matching port name.
        for (int i = 0; i < 3; i++) {
            createPod(portName2, "other-pod" + i, "127.0.0.2", 10000 + i, i > 0);
        }

        // Only Pods with matching port name should not be discovered.
        List<InetSocketAddress> seedNodes = provider1.findSeedNodes(CLUSTER);

        assertEquals(3, seedNodes.size());
        assertTrue(seedNodes.stream().allMatch(address -> address.getHostString().equals("127.0.0.1")));

        // Use different port name.
        KubernetesSeedNodeProvider provider2 = createProvider(portName2);

        seedNodes = provider2.findSeedNodes(CLUSTER);

        assertEquals(3, seedNodes.size());
        assertTrue(seedNodes.stream().allMatch(address -> address.getHostString().equals("127.0.0.2")));
    }

    @Test
    public void testError() throws Exception {
        expectExactMessage(HekateException.class,
            "Kubernetes seed node discovery failure [namespace=test, container-port-name=hekate]",
            () -> new KubernetesSeedNodeProvider(new KubernetesSeedNodeProviderConfig()
                .withTrustCertificates(false) // <-- Must cause failure.
                .withMasterUrl(server.getClient().getMasterUrl().toExternalForm())
                .withNamespace(server.getClient().getNamespace())
            ).findSeedNodes(CLUSTER)
        );
    }

    @Test
    public void testOtherMethods() throws Exception {
        KubernetesSeedNodeProvider provider = createProvider("some-name");

        assertEquals(0, provider.cleanupInterval());

        provider.startDiscovery(CLUSTER, newSocketAddress());
        provider.suspendDiscovery();
        provider.registerRemote(CLUSTER, newSocketAddress());
        provider.unregisterRemote(CLUSTER, newSocketAddress());
        provider.stopDiscovery(CLUSTER, newSocketAddress());

        assertEquals(ToString.format(provider), provider.toString());
    }

    private KubernetesSeedNodeProvider createProvider(String portName) {
        return new KubernetesSeedNodeProvider(new KubernetesSeedNodeProviderConfig()
            .withContainerPortName(portName)
            .withMasterUrl(server.getClient().getMasterUrl().toExternalForm())
            .withNamespace(server.getClient().getNamespace())
            .withTrustCertificates(true)
        );
    }

    private void createPod(String portName, String name, String host, int port, boolean running) {
        Pod pod = new PodBuilder().withNewMetadata()
            .withName(name)
            .endMetadata()
            .withNewStatus().withPodIP(host).withPhase(running ? "Running" : "Pending").endStatus()
            .withNewSpec()
            .addNewContainer().addNewPort().withName(portName).withContainerPort(port).endPort().endContainer()
            .endSpec()
            .build();

        server.getClient().pods().create(pod);
    }
}
