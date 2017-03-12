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

package io.hekate.cluster.seed.jclouds;

import io.hekate.cluster.seed.PersistentSeedNodeProviderCommonTest;
import java.io.File;
import java.nio.file.Files;
import java.util.UUID;
import org.junit.Assume;
import org.junit.BeforeClass;

public class CloudStoreSeedNodeProviderTest extends PersistentSeedNodeProviderCommonTest<CloudStoreSeedNodeProvider> {
    private final String container = "container-" + UUID.randomUUID().toString();

    private File tempDir;

    @BeforeClass
    public static void mayBeDisableTest() {
        String osName = System.getProperty("os.name").toLowerCase();

        // TODO: Temporary disabled due to a bug in the JClouds filesystem blog store on Windows (UserPrincipalNotFoundException)
        Assume.assumeFalse(osName.contains("windows"));
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        tempDir = Files.createTempDirectory("hekate_cloud_seed").toFile();

        say("Using temp directory: " + tempDir.getAbsolutePath());

        new File(tempDir, container).mkdirs();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        deleteDirectory(tempDir);
    }

    @Override
    protected CloudStoreSeedNodeProvider createProvider() throws Exception {
        CloudStoreSeedNodeProviderConfig cfg = new CloudStoreSeedNodeProviderConfig()
            .withCredentials(new BasicCredentialsSupplier().withIdentity("test").withCredential("test"))
            .withProvider("filesystem")
            .withContainer(container)
            .withProperty("jclouds.filesystem.basedir", tempDir.getAbsolutePath())
            .withCleanupInterval(100);

        return new CloudStoreSeedNodeProvider(cfg);
    }
}
