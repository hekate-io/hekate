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

import io.hekate.util.format.ToString;

/**
 * Configuration for {@link KubernetesSeedNodeProvider}.
 *
 * <p>
 * Besides those options that are provided by setters of this class it possible to specify the following system properties and/or
 * environment variables:
 * </p>
 * <ul>
 * <li>{@code kubernetes.master} / {@code KUBERNETES_MASTER}</li>
 * <li>{@code kubernetes.api.version} / {@code KUBERNETES_API_VERSION}</li>
 * <li>{@code kubernetes.oapi.version} / {@code KUBERNETES_OAPI_VERSION}</li>
 * <li>{@code kubernetes.trust.certificates} / {@code KUBERNETES_TRUST_CERTIFICATES}</li>
 * <li>{@code kubernetes.disable.hostname.verification} / {@code KUBERNETES_DISABLE_HOSTNAME_VERIFICATION}</li>
 * <li>{@code kubernetes.certs.ca.file} / {@code KUBERNETES_CERTS_CA_FILE}</li>
 * <li>{@code kubernetes.certs.ca.data} / {@code KUBERNETES_CERTS_CA_DATA}</li>
 * <li>{@code kubernetes.certs.client.file} / {@code KUBERNETES_CERTS_CLIENT_FILE}</li>
 * <li>{@code kubernetes.certs.client.data} / {@code KUBERNETES_CERTS_CLIENT_DATA}</li>
 * <li>{@code kubernetes.certs.client.key.file} / {@code KUBERNETES_CERTS_CLIENT_KEY_FILE}</li>
 * <li>{@code kubernetes.certs.client.key.data} / {@code KUBERNETES_CERTS_CLIENT_KEY_DATA}</li>
 * <li>{@code kubernetes.certs.client.key.algo} / {@code KUBERNETES_CERTS_CLIENT_KEY_ALGO}</li>
 * <li>{@code kubernetes.certs.client.key.passphrase} / {@code KUBERNETES_CERTS_CLIENT_KEY_PASSPHRASE}</li>
 * <li>{@code kubernetes.auth.basic.username} / {@code KUBERNETES_AUTH_BASIC_USERNAME}</li>
 * <li>{@code kubernetes.auth.basic.password} / {@code KUBERNETES_AUTH_BASIC_PASSWORD}</li>
 * <li>{@code kubernetes.auth.tryKubeConfig} / {@code KUBERNETES_AUTH_TRYKUBECONFIG}</li>
 * <li>{@code kubernetes.auth.tryServiceAccount} / {@code KUBERNETES_AUTH_TRYSERVICEACCOUNT}</li>
 * <li>{@code kubernetes.auth.token} / {@code KUBERNETES_AUTH_TOKEN}</li>
 * <li>{@code kubernetes.watch.reconnectInterval} / {@code KUBERNETES_WATCH_RECONNECTINTERVAL}</li>
 * <li>{@code kubernetes.watch.reconnectLimit} / {@code KUBERNETES_WATCH_RECONNECTLIMIT}</li>
 * <li>{@code kubernetes.user.agent} / {@code KUBERNETES_USER_AGENT}</li>
 * <li>{@code kubernetes.tls.versions} / {@code KUBERNETES_TLS_VERSIONS}</li>
 * <li>{@code kubernetes.truststore.file} / {@code KUBERNETES_TRUSTSTORE_FILE}</li>
 * <li>{@code kubernetes.truststore.passphrase} / {@code KUBERNETES_TRUSTSTORE_PASSPHRASE}</li>
 * <li>{@code kubernetes.keystore.file} / {@code KUBERNETES_KEYSTORE_FILE}</li>
 * <li>{@code kubernetes.keystore.passphrase} / {@code KUBERNETES_KEYSTORE_PASSPHRASE}</li>
 * </ul>
 *
 * @see KubernetesSeedNodeProvider#KubernetesSeedNodeProvider(KubernetesSeedNodeProviderConfig)
 */
public class KubernetesSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setContainerPortName(String)}. */
    public static final String DEFAULT_CONTAINER_PORT_NAME = "hekate";

    /** See {@link #setContainerPortName(String)}. */
    private String containerPortName = DEFAULT_CONTAINER_PORT_NAME;

    /** See {@link #setNamespace(String)}. */
    private String namespace;

    /** See {@link #setMasterUrl(String)}. */
    private String masterUrl;

    /** See {@link #setTrustCertificates(Boolean)}. */
    private Boolean trustCertificates;

    /**
     * Returns the port name of a Pod's container (see {@link #setContainerPortName(String)}).
     *
     * @return Port name of a Pod's container.
     */
    public String getContainerPortName() {
        return containerPortName;
    }

    /**
     * Sets the port name of a Pod's container.
     *
     * <p>
     * {@link KubernetesSeedNodeProvider} uses this port name to scan through all of the Pods and filer out those that don't have the
     * specified port. Those containers that have the specified named port will be used as seed node addresses.
     * </p>
     *
     * <p>
     * For example, if there is a Pod of the following definition then the value of this parameter should be set to {@code hekate}:
     * </p>
     * <pre>{@code
     *   apiVersion: v1
     *   kind: Pod
     *   metadata:
     *     name: my-hekate-app
     *   spec:
     *     containers:
     *     - image: my-hekate-app:v1
     *       name: my-hekate-app
     *       ports:
     *       - name: hekate   # <--- This (Hekate cluster port)
     *         containerPort: 10012
     *       - name: http     # ...some other ports...
     *         containerPort: 8080
     * }</pre>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_CONTAINER_PORT_NAME}.
     * </p>
     *
     * @param containerPortName Port name.
     */
    public void setContainerPortName(String containerPortName) {
        this.containerPortName = containerPortName;
    }

    /**
     * Fluent-style version of {@link #setContainerPortName(String)}.
     *
     * @param containerPortName Port name of a Pod's container.
     *
     * @return This instance.
     */
    public KubernetesSeedNodeProviderConfig withContainerPortName(String containerPortName) {
        setContainerPortName(containerPortName);

        return this;
    }

    /**
     * Returns the Kubernetes namespace (see {@link #setNamespace(String)}).
     *
     * @return Kubernetes namespace.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the Kubernetes namespace.
     *
     * <p>
     * If this parameter is not set then the namespace of a Pod that runs the Hekate application will be used by default.
     * </p>
     *
     * @param namespace Kubernetes namespace.
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Fluent-style version of {@link #setNamespace(String)}.
     *
     * @param namespace Kubernetes namespace.
     *
     * @return This instance.
     */
    public KubernetesSeedNodeProviderConfig withNamespace(String namespace) {
        setNamespace(namespace);

        return this;
    }

    /**
     * Returns the URL of Kubernetes API server.
     *
     * @return URL of Kubernetes API server.
     */
    public String getMasterUrl() {
        return masterUrl;
    }

    /**
     * Sets the URL of Kubernetes API server.
     *
     * @param masterUrl URL of Kubernetes API server.
     */
    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    /**
     * Fluent-style version of {@link #setMasterUrl(String)}.
     *
     * @param masterUrl URL of Kubernetes API server.
     *
     * @return This instance.
     */
    public KubernetesSeedNodeProviderConfig withMasterUrl(String masterUrl) {
        setMasterUrl(masterUrl);

        return this;
    }

    /**
     * Returns the flag indicating that Kubernetes API must have a trusted certificate (see {@link #setTrustCertificates(Boolean)}).
     *
     * @return Flag indicating that Kubernetes API must have a trusted certificate.
     */
    public Boolean getTrustCertificates() {
        return trustCertificates;
    }

    /**
     * Sets the flag indicating that Kubernetes API must have a trusted certificate.
     *
     * <p>
     * Setting this parameter to a non-{@code null} value overrides the value of {@code kubernetes.master}/{@code KUBERNETES_MASTER}
     * system/environment variable.
     * </p>
     *
     * @param trustCertificates Flag indicating that Kubernetes API must have a trusted certificate.
     */
    public void setTrustCertificates(Boolean trustCertificates) {
        this.trustCertificates = trustCertificates;
    }

    /**
     * Fluent-style version of {@link #setTrustCertificates(Boolean)}.
     *
     * @param trustCertificates Flag indicating that Kubernetes API must have a trusted certificate.
     *
     * @return This instance.
     */
    public KubernetesSeedNodeProviderConfig withTrustCertificates(Boolean trustCertificates) { 
        setTrustCertificates(trustCertificates);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
