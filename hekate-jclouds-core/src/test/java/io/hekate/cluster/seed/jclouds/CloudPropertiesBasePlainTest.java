/*
 * Copyright 2022 The Hekate Project
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

import java.util.Properties;
import org.jclouds.Constants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CloudPropertiesBasePlainTest extends CloudPropertiesBaseTestBase {
    @Test
    public void testEmpty() {
        CloudPropertiesBase config = createConfig();

        assertTrue(config.buildBaseProperties().isEmpty());
    }

    @Test
    public void testProperties() {
        CloudPropertiesBase config = createConfig()
            .withConnectTimeout(1050)
            .withSoTimeout(100500);

        Properties props = config.buildBaseProperties();

        assertEquals("1050", props.getProperty(Constants.PROPERTY_CONNECTION_TIMEOUT));
        assertEquals("100500", props.getProperty(Constants.PROPERTY_SO_TIMEOUT));
    }

    @Override
    protected CloudPropertiesBase createConfig() {
        return new CloudPropertiesBase() {
            // No-op.
        };
    }
}
