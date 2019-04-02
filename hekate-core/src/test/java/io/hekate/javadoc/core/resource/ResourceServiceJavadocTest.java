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

package io.hekate.javadoc.core.resource;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.resource.ResourceService;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ResourceServiceJavadocTest extends HekateNodeTestBase {
    @Test
    public void example() throws Exception {
        Hekate hekate = createNode().join();

        // Start:access
        ResourceService resources = hekate.get(ResourceService.class);
        // End:access

        assertNotNull(resources);
    }
}
