/*
 * Copyright 2020 The Hekate Project
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

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static io.hekate.core.internal.util.Utils.nullOrTrim;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

/**
 * Kubernetes-based implementation of {@link SeedNodeProvider} interface.
 *
 * <h2>Overview</h2>
 * <p>
 * This provider uses Kubernetes API server to search for all Pods that have a specially named port (Hekate cluster port). Name of that
 * port can be specified via the {@link KubernetesSeedNodeProviderConfig#setContainerPortName(String)} configuration property (default
 * value is defined by {@link KubernetesSeedNodeProviderConfig#DEFAULT_CONTAINER_PORT_NAME}).
 * </p>
 *
 * <p>
 * Example of Pod definition:
 * ${source: kubernetes-example.yaml#pod_example}
 * </p>
 *
 * <p>
 * Note that Pods can have different containers of different types that run different applications, but if such applications must form a
 * single Hekate cluster then all of them should use the same name of Hekate cluster port.
 * </p>
 *
 * <h2>Configuration</h2>
 * <p>
 * Please see the documentation of {@link KubernetesSeedNodeProviderConfig} class for details about the available configuration options.
 * </p>
 *
 * <h2>Kubernetes Role-based Access Control (RBAC)</h2>
 * <p>
 * If Kubernetes cluster is running with Role-based Access Control enabled then it is important to make sure that Pod has permissions to
 * read information about other Pods from Kubernetes API server. It can be done by creating an RBAC Role and binding it to the Pod's Service
 * Account.
 * </p>
 *
 * <p>
 * The following example provides a basic example of granting permissions for a Service Account to read information about other Pods:
 * ${source: kubernetes-example.yaml#role_example}
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class KubernetesSeedNodeProvider implements SeedNodeProvider, ConfigReportSupport {
    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(KubernetesSeedNodeProvider.class);

    /** Statuses of Pods that can be selected as seed nodes. */
    private static final Set<String> ACTIVE_POD_PHASES = unmodifiableSet(new HashSet<>(asList("Pending", "Running")));

    /** Configuration of Fabric8.io client for Kubernetes. */
    @ToStringIgnore
    private final Config config;

    /** See {@link KubernetesSeedNodeProviderConfig#setContainerPortName(String)}. */
    private final String containerPortName;

    /** See {@link KubernetesSeedNodeProviderConfig#setMasterUrl(String)}. */
    private final String masterUrl;

    /** See {@link KubernetesSeedNodeProviderConfig#setNamespace(String)}. */
    private final String namespace;

    /** See {@link KubernetesSeedNodeProviderConfig#setTrustCertificates(Boolean)}. */
    private final Boolean trustCertificates;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public KubernetesSeedNodeProvider(KubernetesSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        this.containerPortName = nullOrTrim(cfg.getContainerPortName());
        masterUrl = cfg.getMasterUrl();
        namespace = cfg.getNamespace();
        trustCertificates = cfg.getTrustCertificates();

        ConfigCheck check = ConfigCheck.get(KubernetesSeedNodeProviderConfig.class);

        check.notEmpty(containerPortName, "container port name");

        ConfigBuilder builder = new ConfigBuilder();

        Optional.ofNullable(nullOrTrim(masterUrl)).ifPresent(builder::withMasterUrl);
        Optional.ofNullable(nullOrTrim(namespace)).ifPresent(builder::withNamespace);
        Optional.ofNullable(trustCertificates).ifPresent(builder::withTrustCerts);

        this.config = builder.build();
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("kubernetes", r -> {
            r.value("container-port-name", containerPortName);
            r.value("master-url", masterUrl);
            r.value("namespace", namespace);
            r.value("trust-certificates", trustCertificates);
        });
    }

    /**
     * Container port name.
     *
     * @return Container port name.
     *
     * @see KubernetesSeedNodeProviderConfig#setContainerPortName(String)
     */
    public String containerPortName() {
        return containerPortName;
    }

    /**
     * Kubernetes master URL.
     *
     * @return Master URL.
     *
     * @see KubernetesSeedNodeProviderConfig#setMasterUrl(String)
     */
    public String masterUrl() {
        return masterUrl;
    }

    /**
     * Kubernetes namespace.
     *
     * @return Kubernetes namespace.
     *
     * @see KubernetesSeedNodeProviderConfig#setNamespace(String)
     */
    public String namespace() {
        return namespace;
    }

    /**
     * {@code true} if  Kubernetes API must have a trusted certificate.
     *
     * @return {@code true} if  Kubernetes API must have a trusted certificate.
     *
     * @see KubernetesSeedNodeProviderConfig#setTrustCertificates(Boolean)
     */
    public Boolean trustCertificates() {
        return trustCertificates;
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String namespace) throws HekateException {
        if (log.isDebugEnabled()) {
            log.debug("Searching for seed node addresses [namespace={}, container-port-name={}]", config.getNamespace(), containerPortName);
        }

        try (KubernetesClient client = new DefaultKubernetesClient(config)) {
            List<InetSocketAddress> seedNodes = new ArrayList<>();

            PodList pods = client.pods().list();

            // Search through Pods -> Containers -> Ports.
            nullSafe(pods.getItems())
                .filter(pod -> pod.getSpec() != null && pod.getStatus() != null && pod.getStatus().getPodIP() != null)
                .filter(pod -> ACTIVE_POD_PHASES.contains(pod.getStatus().getPhase()))
                .forEach(pod -> {
                    String ip = pod.getStatus().getPodIP();

                    nullSafe(pod.getSpec().getContainers())
                        .flatMap(container -> nullSafe(container.getPorts()))
                        .filter(port -> containerPortName.equals(port.getName()) && port.getContainerPort() != null)
                        .map(ContainerPort::getContainerPort)
                        .forEach(port ->
                            seedNodes.add(new InetSocketAddress(ip, port))
                        );
                });

            return seedNodes;
        } catch (KubernetesClientException e) {
            throw new HekateException("Kubernetes seed node discovery failure ["
                + "namespace=" + config.getNamespace() + ", "
                + "container-port-name=" + containerPortName
                + "]", e);
        }
    }

    @Override
    public void startDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        if (log.isInfoEnabled()) {
            log.info("Started seed node discovery [namespace={}, container-port-name={}]", config.getNamespace(), containerPortName);
        }
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    @Override
    public void stopDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public long cleanupInterval() {
        return 0;
    }

    @Override
    public void registerRemote(String namespace, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void unregisterRemote(String namespace, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
