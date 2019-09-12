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

package io.hekate.network;

import io.hekate.util.format.ToString;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLSessionContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * SSL configuration options.
 *
 * @see NetworkServiceFactory#setSsl(NetworkSslConfig)
 */
public class NetworkSslConfig {
    /**
     * SSL provider type.
     */
    public enum Provider {
        /** Automatically select provider depending on what is available on the classpath. */
        AUTO,

        /** JDK default SSL provider. */
        JDK,

        /** OpenSSL provider. */
        OPEN_SSL
    }

    private Provider provider = Provider.AUTO;

    private String keyStoreAlgorithm;

    private String keyStorePath;

    private String keyStoreType;

    private String keyStorePassword;

    private String trustStoreAlgorithm;

    private String trustStorePath;

    private String trustStoreType;

    private String trustStorePassword;

    private int sslSessionCacheSize;

    private int sslSessionCacheTimeout;

    /**
     * Returns the SSL provider (see {@link #setProvider(Provider)}).
     *
     * @return SSL provider.
     */
    public Provider getProvider() {
        return provider;
    }

    /**
     * Sets the SSL provider.
     *
     * <p>
     * Default value of this parameter is {@link Provider#AUTO}.
     * </p>
     *
     * @param provider SSL provider.
     */
    public void setProvider(Provider provider) {
        this.provider = provider;
    }

    /**
     * Fluent-style version of {@link #setProvider(Provider)}.
     *
     * @param provider SSL provider.
     *
     * @return This instance.
     */
    public NetworkSslConfig withProvider(Provider provider) {
        setProvider(provider);

        return this;
    }

    /**
     * Returns the keystore algorithm (see {@link #setKeyStoreAlgorithm(String)}).
     *
     * @return Keystore algorithm.
     */
    public String getKeyStoreAlgorithm() {
        return keyStoreAlgorithm;
    }

    /**
     * Sets the keystore algorithm.
     *
     * <p>
     * If not specified then the JDK default algorithm will be used (see {@link KeyManagerFactory#getDefaultAlgorithm()}).
     * </p>
     *
     * @param keyStoreAlgorithm Keystore algorithm.
     */
    public void setKeyStoreAlgorithm(String keyStoreAlgorithm) {
        this.keyStoreAlgorithm = keyStoreAlgorithm;
    }

    /**
     * Fluent-style version of {@link #setKeyStoreAlgorithm(String)}.
     *
     * @param keyStoreAlgorithm Keystore algorithm.
     *
     * @return This instance.
     */
    public NetworkSslConfig withKeyStoreAlgorithm(String keyStoreAlgorithm) {
        setKeyStoreAlgorithm(keyStoreAlgorithm);

        return this;
    }

    /**
     * Returns the keystore path (see {@link #setKeyStorePath(String)}).
     *
     * @return Keystore path.
     */
    public String getKeyStorePath() {
        return keyStorePath;
    }

    /**
     * Sets the path to the keystore file.
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * @param keyStorePath Keystore path.
     */
    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    /**
     * Fluent-style version of {@link #setKeyStorePath(String)}.
     *
     * @param keyStorePath Keystore path.
     *
     * @return This instance.
     */
    public NetworkSslConfig withKeyStorePath(String keyStorePath) {
        setKeyStorePath(keyStorePath);

        return this;
    }

    /**
     * Returns the keystore type (see {@link #setKeyStoreType(String)}).
     *
     * @return Keystore type.
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Sets the keystore type.
     *
     * <p>
     * If not specified then the JDK default type will be used (see {@link KeyStore#getDefaultType()}).
     * </p>
     *
     * @param keyStoreType Keystore type.
     */
    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    /**
     * Fluent-style version of {@link #setKeyStoreType(String)}.
     *
     * @param keyStoreType Keystore type.
     *
     * @return This instance.
     */
    public NetworkSslConfig withKeyStoreType(String keyStoreType) {
        setKeyStoreType(keyStoreType);

        return this;
    }

    /**
     * Returns the keystore password (see {@link #setKeyStorePassword(String)}).
     *
     * @return Keystore password.
     */
    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * Sets the keystore password.
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * @param keyStorePassword Keystore password.
     */
    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    /**
     * Fluent-style version of {@link #setKeyStorePassword(String)}.
     *
     * @param keyStorePassword Keystore password.
     *
     * @return This instance.
     */
    public NetworkSslConfig withKeyStorePassword(String keyStorePassword) {
        setKeyStorePassword(keyStorePassword);

        return this;
    }

    /**
     * Returns the trust store path (see {@link #setKeyStorePath(String)}).
     *
     * @return Trust store path.
     */
    public String getTrustStorePath() {
        return trustStorePath;
    }

    /**
     * Sets the path to the trust store file.
     *
     * <p>
     * If not specified then an insecure {@link TrustManagerFactory} that trusts all X.509 certificates without any verification will be
     * used.
     * </p>
     *
     * @param trustStorePath Trust store path.
     */
    public void setTrustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
    }

    /**
     * Fluent-style version of {@link #setTrustStorePath(String)}.
     *
     * @param trustStorePath Trust store path.
     *
     * @return This instance.
     */
    public NetworkSslConfig withTrustStorePath(String trustStorePath) {
        setTrustStorePath(trustStorePath);

        return this;
    }

    /**
     * Returns the trust store type (see {@link #setTrustStoreType(String)}).
     *
     * @return Trust store type.
     */
    public String getTrustStoreType() {
        return trustStoreType;
    }

    /**
     * Sets the trust store type.
     *
     * <p>
     * If not specified then the JDK default type will be used (see {@link KeyStore#getDefaultType()}).
     * </p>
     *
     * @param trustStoreType Trust store type.
     */
    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    /**
     * Fluent-style version of {@link #setTrustStoreType(String)}.
     *
     * @param trustStoreType Trust store type.
     *
     * @return This instance.
     */
    public NetworkSslConfig withTrustStoreType(String trustStoreType) {
        setTrustStoreType(trustStoreType);

        return this;
    }

    /**
     * Returns the trust store password (see {@link #setTrustStorePassword(String)}).
     *
     * @return Trust store password.
     */
    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * Sets the trust store password.
     *
     * <p>
     * This parameter is mandatory only if {@link #setTrustStorePath(String)} is specified.
     * </p>
     *
     * @param trustStorePassword Trust store password.
     */
    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    /**
     * Fluent-style version of {@link #setTrustStorePassword(String)}.
     *
     * @param trustStorePassword Trust store password.
     *
     * @return This instance.
     */
    public NetworkSslConfig withTrustStorePassword(String trustStorePassword) {
        setTrustStorePassword(trustStorePassword);

        return this;
    }

    /**
     * Returns the trust store algorithm (see {@link #setTrustStoreAlgorithm(String)}).
     *
     * @return Trust store algorithm.
     */
    public String getTrustStoreAlgorithm() {
        return trustStoreAlgorithm;
    }

    /**
     * Sets the trust store algorithm.
     *
     * <p>
     * If not specified then the JDK default algorithm will be used (see {@link TrustManagerFactory#getDefaultAlgorithm()}).
     * </p>
     *
     * @param trustStoreAlgorithm Trust store algorithm.
     */
    public void setTrustStoreAlgorithm(String trustStoreAlgorithm) {
        this.trustStoreAlgorithm = trustStoreAlgorithm;
    }

    /**
     * Fluent-style version of {@link #setTrustStoreAlgorithm(String)}.
     *
     * @param trustStoreAlgorithm Trust store algorithm.
     *
     * @return This instance.
     */
    public NetworkSslConfig withTrustStoreAlgorithm(String trustStoreAlgorithm) {
        setTrustStoreAlgorithm(trustStoreAlgorithm);

        return this;
    }

    /**
     * Returns the size of the cache used for storing SSL session objects (see {@link #setSslSessionCacheSize(int)}).
     *
     * @return Size of SSL session objects cache.
     */
    public int getSslSessionCacheSize() {
        return sslSessionCacheSize;
    }

    /**
     * Set the size of the cache used for storing SSL session objects ({@code 0} to use the JDK default value).
     *
     * @param sslSessionCacheSize Size of SSL session objects cache.
     *
     * @see SSLSessionContext#setSessionCacheSize(int)
     */
    public void setSslSessionCacheSize(int sslSessionCacheSize) {
        this.sslSessionCacheSize = sslSessionCacheSize;
    }

    /**
     * Fluent-style version of {@link #setSslSessionCacheSize(int)}.
     *
     * @param sslSessionCacheSize Size of SSL session objects cache.
     *
     * @return This instance.
     */
    public NetworkSslConfig withSslSessionCacheSize(int sslSessionCacheSize) {
        setSslSessionCacheSize(sslSessionCacheSize);

        return this;
    }

    /**
     * Returns the timeout in seconds for the cached SSL session objects.
     *
     * @return Timeout in seconds for the cached SSL session objects.
     */
    public int getSslSessionCacheTimeout() {
        return sslSessionCacheTimeout;
    }

    /**
     * Sets the timeout in seconds for the cached SSL session objects.
     *
     * @param sslSessionCacheTimeout Timeout in seconds for the cached SSL session objects.
     */
    public void setSslSessionCacheTimeout(int sslSessionCacheTimeout) {
        this.sslSessionCacheTimeout = sslSessionCacheTimeout;
    }

    /**
     * Fluent-style version of {@link #setSslSessionCacheTimeout(int)}.
     *
     * @param sslSessionCacheTimeout Timeout in seconds for the cached SSL session objects.
     *
     * @return This instance.
     */
    public NetworkSslConfig withSslSessionCacheTimeout(int sslSessionCacheTimeout) {
        setSslSessionCacheTimeout(sslSessionCacheTimeout);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
