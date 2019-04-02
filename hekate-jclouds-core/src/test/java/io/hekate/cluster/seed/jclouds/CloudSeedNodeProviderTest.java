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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.jclouds.location.reference.LocationConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.jclouds.ContextBuilder.newBuilder;
import static org.jclouds.compute.options.TemplateOptions.Builder.userMetadata;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class CloudSeedNodeProviderTest extends HekateTestBase {
    private final CloudTestContext testCtx;

    public CloudSeedNodeProviderTest(CloudTestContext testCtx) {
        this.testCtx = testCtx;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<CloudTestContext> getCloudTestContexts() {
        return CloudTestContext.allContexts();
    }

    @BeforeClass
    public static void setUpClass() throws RunNodesException {
        // Disable if there are no cloud providers that are configured for tests.
        Assume.assumeFalse(getCloudTestContexts().isEmpty());

        Collection<CloudTestContext> contexts = getCloudTestContexts();

        for (CloudTestContext ctx : contexts) {
            sayHeader("Setting up '" + ctx.computeProvider() + "' instances.");

            Properties props = new Properties();

            props.setProperty(LocationConstants.PROPERTY_REGIONS, ctx.autoCreateRegion());

            ContextBuilder builder = newBuilder(ctx.computeProvider()).credentials(
                ctx.identity(),
                ctx.credential()
            ).overrides(props);

            try (ComputeServiceContext computeCtx = builder
                .modules(ImmutableSet.<Module>of(new SLF4JLoggingModule()))
                .buildView(ComputeServiceContext.class)
            ) {
                for (int i = 0; i < 4; i++) {
                    ensureNodeExists(i, ctx.autoCreateRegion(), computeCtx.getComputeService());
                }
            }
        }
    }

    @Test
    public void test() throws Exception {
        CloudSeedNodeProvider provider = provider();

        List<InetSocketAddress> nodes = provider.findSeedNodes("");

        assertTrue(nodes.size() >= 4);
    }

    @Test
    public void testTagMatch() throws Exception {
        CloudSeedNodeProvider tag1 = provider(cfg ->
            cfg.withTag("HekateTestTag1", "Tag1Test")
        );

        CloudSeedNodeProvider tag2 = provider(cfg ->
            cfg.withTag("HekateTestTag2", "Tag2Test")
        );

        List<InetSocketAddress> nodes1 = tag1.findSeedNodes("");
        List<InetSocketAddress> node2 = tag2.findSeedNodes("");

        assertEquals(2, nodes1.size());
        assertEquals(2, node2.size());
        assertNotEquals(new HashSet<>(nodes1), new HashSet<>(node2));
    }

    @Test
    public void testTagNotMatch() throws Exception {
        CloudSeedNodeProvider provider = provider(cfg -> {
            cfg.withTag(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            cfg.withTag("HekateTestTag2", "Tag2Test");
        });

        assertTrue(provider.findSeedNodes("").isEmpty());
    }

    @Test
    public void testRegionNotMatch() throws Exception {
        CloudSeedNodeProvider provider = provider(cfg ->
            cfg.setRegions(Collections.singleton(selectUnusedRegion()))
        );

        assertTrue(provider.findSeedNodes("").isEmpty());
    }

    @Test
    public void testMixedRegions() throws Exception {
        CloudSeedNodeProvider provider = provider(cfg -> {
            cfg.withRegion(selectUnusedRegion());
            cfg.withRegion(testCtx.region());
        });

        assertEquals(4, provider.findSeedNodes("").size());
    }

    @Test
    public void testEmptyMethods() throws Exception {
        CloudSeedNodeProvider provider = provider();

        assertEquals(0, provider.cleanupInterval());

        provider.suspendDiscovery();
        provider.registerRemote(null, null);
        provider.unregisterRemote(null, null);
        provider.stopDiscovery(null, null);
    }

    @Test
    public void testToString() throws Exception {
        CloudSeedNodeProvider provider = provider();

        assertEquals(ToString.format(provider), provider.toString());
    }

    private CloudSeedNodeProvider provider() throws Exception {
        return provider(null);
    }

    private CloudSeedNodeProvider provider(Consumer<CloudSeedNodeProviderConfig> configurer) throws Exception {
        CloudSeedNodeProviderConfig cfg = new CloudSeedNodeProviderConfig()
            .withProvider(testCtx.computeProvider())
            .withRegion(testCtx.region())
            .withConnectTimeout(3000)
            .withSoTimeout(3000)
            .withTag("Test", "true")
            .withCredentials(new BasicCredentialsSupplier()
                .withIdentity(testCtx.identity())
                .withCredential(testCtx.credential())
            );

        if (configurer != null) {
            configurer.accept(cfg);
        }

        CloudSeedNodeProvider provider = new CloudSeedNodeProvider(cfg) {
            @Override
            boolean acceptState(NodeMetadata node) {
                // For testing purposes we accept all instances (even those that are stopped).
                return true;
            }
        };

        provider.startDiscovery("", newSocketAddress());

        return provider;
    }

    private String selectUnusedRegion() {
        return Stream.of(testCtx.allRegions())
            .filter(r -> !r.equals(testCtx.region()))
            .findFirst()
            .orElseThrow(AssertionError::new);
    }

    private static void ensureNodeExists(int idx, String region, ComputeService compute) throws RunNodesException {
        Map<String, String> tags = new HashMap<>();

        String name = "HekateUnitTest-" + idx;

        tags.put("Name", name);
        tags.put("Test", "true");

        String tagName;
        String tagValue;

        if (idx % 2 == 0) {
            tagName = "HekateTestTag1";
            tagValue = "Tag1Test";
        } else {
            tagName = "HekateTestTag2";
            tagValue = "Tag2Test";
        }

        tags.put(tagName, tagValue);

        boolean create = compute.listNodesDetailsMatching(node -> {
            if (node != null) {
                Map<String, String> userMeta = node.getUserMetadata();

                if (userMeta != null) {
                    for (Map.Entry<String, String> e : tags.entrySet()) {
                        if (!Objects.equals(userMeta.get(e.getKey()), e.getValue())) {
                            return false;
                        }
                    }

                    return true;
                }
            }

            return false;
        }).isEmpty();

        if (create) {
            Template template = compute.templateBuilder()
                .locationId(region)
                .smallest()
                .osFamily(OsFamily.DEBIAN)
                .options(userMetadata(tags))
                .build();

            say("Creating a new instance [tags=" + tags + ']');

            NodeMetadata node = compute.createNodesInGroup("hekate-test", 1, template).iterator().next();

            say("Instance created [id=" + node.getId() + ']');

            say("Stopping instance [id=" + node.getId() + ']');

            compute.suspendNode(node.getId());

            say("Instance stopped [id=" + node.getId() + ']');
        } else {
            say("Instance exists: " + tags);
        }
    }
}
