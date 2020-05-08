/*
 * Copyright 2020 The Hekate Project
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

import io.hekate.HekateTestBase;
import java.util.Properties;
import org.jclouds.Constants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public abstract class CloudPropertiesBaseTestBase extends HekateTestBase {
    protected abstract CloudPropertiesBase createConfig();

    @Test
    public void testEmptyBaseProperties() {
        CloudPropertiesBase cfg = createConfig();

        Properties props = cfg.buildBaseProperties();

        assertEquals(0, props.size());
    }

    @Test
    public void testConnectTimeout() {
        CloudPropertiesBase cfg = createConfig();

        assertNull(cfg.getConnectTimeout());

        cfg.setConnectTimeout(10001);

        assertEquals(10001, cfg.getConnectTimeout().longValue());

        assertSame(cfg, cfg.withConnectTimeout(10002));

        assertEquals(10002, cfg.getConnectTimeout().longValue());

        Properties props = cfg.buildBaseProperties();

        assertEquals(1, props.size());
        assertEquals("10002", props.getProperty(Constants.PROPERTY_CONNECTION_TIMEOUT));
    }

    @Test
    public void testSoTimeout() {
        CloudPropertiesBase cfg = createConfig();

        assertNull(cfg.getSoTimeout());

        cfg.setSoTimeout(10001);

        assertEquals(10001, cfg.getSoTimeout().longValue());

        assertSame(cfg, cfg.withSoTimeout(10002));

        assertEquals(10002, cfg.getSoTimeout().longValue());

        Properties props = cfg.buildBaseProperties();

        assertEquals(1, props.size());
        assertEquals("10002", props.getProperty(Constants.PROPERTY_SO_TIMEOUT));
    }
}
