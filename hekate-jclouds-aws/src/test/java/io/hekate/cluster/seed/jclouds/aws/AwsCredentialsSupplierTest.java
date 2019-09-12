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

import io.hekate.HekateTestBase;
import java.util.UUID;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AwsCredentialsSupplierTest extends HekateTestBase {
    public static final String AWS_ACCESS_KEY_ID = "aws.accessKeyId";

    public static final String AWS_SECRET_KEY = "aws.secretKey";

    @Test
    public void test() throws Exception {
        String oldAccessKey = null;
        String oldSecretKey = null;

        try {
            oldAccessKey = System.getProperty(AWS_ACCESS_KEY_ID);
            oldSecretKey = System.getProperty(AWS_SECRET_KEY);

            String fakeAccessKey = UUID.randomUUID().toString();
            String fakeSecretKey = UUID.randomUUID().toString();

            System.setProperty(AWS_ACCESS_KEY_ID, fakeAccessKey);
            System.setProperty(AWS_SECRET_KEY, fakeSecretKey);

            AwsCredentialsSupplier supplier = new AwsCredentialsSupplier();

            assertEquals(fakeAccessKey, supplier.get().identity);
            assertEquals(fakeSecretKey, supplier.get().credential);

            supplier.withCredential("test-secret");

            assertEquals(fakeAccessKey, supplier.get().identity);
            assertEquals(fakeSecretKey, supplier.get().credential);

            supplier.withIdentity("test-access");

            assertEquals("test-access", supplier.get().identity);
            assertEquals("test-secret", supplier.get().credential);
        } finally {
            if (oldAccessKey != null) {
                System.setProperty(AWS_ACCESS_KEY_ID, oldAccessKey);
            }

            if (oldSecretKey != null) {
                System.setProperty(AWS_SECRET_KEY, oldSecretKey);
            }
        }
    }
}
