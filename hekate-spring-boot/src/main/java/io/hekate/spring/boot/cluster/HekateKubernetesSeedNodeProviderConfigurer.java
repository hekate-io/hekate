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

package io.hekate.spring.boot.cluster;

import io.hekate.cluster.seed.kubernetes.KubernetesSeedNodeProvider;
import io.hekate.cluster.seed.kubernetes.KubernetesSeedNodeProviderConfig;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for {@link KubernetesSeedNodeProvider}.
 *
 * <p>
 * This auto-configuration is disabled by default and can be enabled by setting the {@code 'hekate.cluster.seed.kubernetes.enable'}
 * property to {@code true} in the application configuration.
 * </p>
 *
 * <p>
 * The following properties can be used to customize the auto-configured {@link KubernetesSeedNodeProvider} instance:
 * </p>
 * <ul>
 * <li>{@link KubernetesSeedNodeProviderConfig#setContainerPortName(String) 'hekate.cluster.seed.kubernetes.container-port-name'}</li>
 * <li>{@link KubernetesSeedNodeProviderConfig#setMasterUrl(String) 'hekate.cluster.seed.kubernetes.master-url'}</li>
 * <li>{@link KubernetesSeedNodeProviderConfig#setNamespace(String) 'hekate.cluster.seed.kubernetes.namespace'}</li>
 * </ul>
 *
 * <p>
 * Additionally it is possible to specify the following system properties and/or environment variables:
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
 * @see HekateClusterServiceConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateClusterServiceConfigurer.class)
@ConditionalOnProperty(value = "hekate.cluster.seed.kubernetes.enable", havingValue = "true")
public class HekateKubernetesSeedNodeProviderConfigurer {
    /**
     * Conditionally constructs a new configuration for {@link KubernetesSeedNodeProvider} if application doesn't provide its own
     * {@link Bean} of {@link KubernetesSeedNodeProviderConfig} type.
     *
     * @return New configuration.
     */
    @Bean
    @ConditionalOnMissingBean(KubernetesSeedNodeProviderConfig.class)
    @ConfigurationProperties(prefix = "hekate.cluster.seed.kubernetes")
    public KubernetesSeedNodeProviderConfig kubernetesSeedNodeProviderConfig() {
        return new KubernetesSeedNodeProviderConfig();
    }

    /**
     * Constructs new {@link KubernetesSeedNodeProvider}.
     *
     * @param cfg Configuration (see {@link #kubernetesSeedNodeProviderConfig()}).
     *
     * @return New provider.
     */
    @Bean
    public KubernetesSeedNodeProvider kubernetesSeedNodeProvider(KubernetesSeedNodeProviderConfig cfg) {
        return new KubernetesSeedNodeProvider(cfg);
    }
}
