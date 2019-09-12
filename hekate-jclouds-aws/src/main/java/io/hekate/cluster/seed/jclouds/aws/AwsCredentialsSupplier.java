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
 * of AWS SDK for Java). Below is the quote of its documentation for fast reference:
 * </p>
 *
 * <p>AWS credentials provider chain that looks for credentials in this order:</p>
 *
 * <ul>
 * <li>Environment Variables - {@code AWS_ACCESS_KEY_ID} and {@code AWS_SECRET_ACCESS_KEY} (RECOMMENDED since they are recognized by all the
 * AWS SDKs and CLI except for .NET), or {@code AWS_ACCESS_KEY} and {@code AWS_SECRET_KEY} (only recognized by Java SDK)
 * </li>
 * <li>Java System Properties - aws.accessKeyId and aws.secretKey</li>
 * <li>Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI</li>
 * <li>Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is
 * set and security manager has permission to access the variable,</li>
 * <li>Instance profile credentials delivered through the Amazon EC2 metadata service</li>
 * </ul>
 *
 * @see CloudSeedNodeProviderConfig#setCredentials(CredentialsSupplier)
 * @see CloudStoreSeedNodeProviderConfig#setCredentials(CredentialsSupplier)
 */
public class AwsCredentialsSupplier extends BasicCredentialsSupplier {
    @Override
    public Credentials get() {
        String identity = getIdentity() != null ? getIdentity().trim() : null;
        String credential = getCredential() != null ? getCredential().trim() : null;

        if (identity == null || identity.isEmpty() || credential == null || credential.isEmpty()) {
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
