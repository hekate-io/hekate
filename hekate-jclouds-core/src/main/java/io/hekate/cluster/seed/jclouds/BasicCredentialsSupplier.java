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

package io.hekate.cluster.seed.jclouds;

import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import org.jclouds.domain.Credentials;

/**
 * Basic credentials supplier for <a href="http://jclouds.apache.org/" target="_blank">Apache JClouds</a>-based seed node providers.
 *
 * @see CloudSeedNodeProviderConfig#setCredentials(CredentialsSupplier)
 * @see CloudStoreSeedNodeProviderConfig#setCredentials(CredentialsSupplier)
 */
public class BasicCredentialsSupplier implements CredentialsSupplier {
    private String identity;

    @ToStringIgnore
    private String credential;

    /**
     * Returns the identity (see {@link #setIdentity(String)}).
     *
     * @return Identity.
     */
    public String getIdentity() {
        return identity;
    }

    /**
     * Sets the identity.
     *
     * @param identity Identity.
     */
    public void setIdentity(String identity) {
        this.identity = identity;
    }

    /**
     * Fluent style version of {@link #setIdentity(String)}.
     *
     * @param identity Identity.
     *
     * @return This instance.
     */
    public BasicCredentialsSupplier withIdentity(String identity) {
        setIdentity(identity);

        return this;
    }

    /**
     * Returns the credential (see {@link #setCredential(String)}).
     *
     * @return Credential.
     */
    public String getCredential() {
        return credential;
    }

    /**
     * Sets the credential.
     *
     * @param credential Credential.
     */
    public void setCredential(String credential) {
        this.credential = credential;
    }

    /**
     * Fluent style version of {@link #setCredential(String)}.
     *
     * @param credential Credential.
     *
     * @return This instance.
     */
    public BasicCredentialsSupplier withCredential(String credential) {
        setCredential(credential);

        return this;
    }

    /**
     * Builds credentials based on {@link #setIdentity(String)} and {@link #setCredential(String)}.
     *
     * @return Credentials.
     */
    @Override
    public Credentials get() {
        return new Credentials.Builder<>().identity(identity).credential(credential).build();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
