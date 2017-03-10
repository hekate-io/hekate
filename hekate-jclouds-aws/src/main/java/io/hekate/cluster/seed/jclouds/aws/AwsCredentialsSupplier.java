/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.cluster.seed.jclouds.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.hekate.cluster.seed.jclouds.BasicCredentialsSupplier;
import io.hekate.cluster.seed.jclouds.CloudSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.CloudStoreSeedNodeProviderConfig;
import io.hekate.cluster.seed.jclouds.CredentialsSupplier;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.domain.Credentials;

/**
 * <p>
 * AWS credentials supplier for {@link io.hekate.cluster.seed.jclouds Apache JClouds}-based seed node providers.
 * </p>
 *
 * <ul>
 * <li>{@link #setIdentity(String) setIdentity(String)} - sets AWS access key</li>
 * <li>{@link #setCredential(String) setCredential(String)} - sets AWS secrete key</li>
 * </ul>
 *
 * <p>
 * If those options are not specified then this class tries to resolve AWS credentials via {@link DefaultAWSCredentialsProviderChain} (part
 * of AWS SDK for Java).
 * </p>
 *
 * @see CloudSeedNodeProviderConfig#setCredentials(CredentialsSupplier)
 * @see CloudStoreSeedNodeProviderConfig#setCredentials(CredentialsSupplier)
 */
public class AwsCredentialsSupplier extends BasicCredentialsSupplier {
    @Override
    public Credentials get() {
        if (getIdentity() == null || getIdentity().trim().isEmpty() || getCredential() == null || getCredential().trim().isEmpty()) {
            DefaultAWSCredentialsProviderChain chain = new DefaultAWSCredentialsProviderChain();

            AWSCredentials cred = chain.getCredentials();

            if (cred instanceof BasicSessionCredentials) {
                BasicSessionCredentials sesCred = (BasicSessionCredentials)cred;

                return new SessionCredentials.Builder()
                    .identity(sesCred.getAWSAccessKeyId())
                    .credential(sesCred.getAWSSecretKey())
                    .sessionToken(sesCred.getSessionToken())
                    .build();
            } else {
                return new Credentials.Builder<>()
                    .identity(cred.getAWSAccessKeyId())
                    .credential(cred.getAWSSecretKey())
                    .build();
            }
        }

        return super.get();
    }
}
