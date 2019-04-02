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

package io.hekate.cluster.seed.kubernetes;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class KubernetesSeedNodeProviderConfigTest extends HekateTestBase {
    private final KubernetesSeedNodeProviderConfig cfg = new KubernetesSeedNodeProviderConfig();

    @Test
    public void testContainerPortName() {
        assertEquals(KubernetesSeedNodeProviderConfig.DEFAULT_CONTAINER_PORT_NAME, cfg.getContainerPortName());

        cfg.setContainerPortName("test");

        assertEquals("test", cfg.getContainerPortName());

        cfg.setContainerPortName(null);

        assertNull(cfg.getContainerPortName());

        assertSame(cfg, cfg.withContainerPortName("test"));

        assertEquals("test", cfg.getContainerPortName());
    }

    @Test
    public void testNamespace() {
        assertNull(cfg.getNamespace());

        cfg.setNamespace("test");

        assertEquals("test", cfg.getNamespace());

        cfg.setNamespace(null);

        assertNull(cfg.getNamespace());

        assertSame(cfg, cfg.withNamespace("test"));

        assertEquals("test", cfg.getNamespace());
    }

    @Test
    public void testMasterUrl() {
        assertNull(cfg.getMasterUrl());

        cfg.setMasterUrl("test");

        assertEquals("test", cfg.getMasterUrl());

        cfg.setMasterUrl(null);

        assertNull(cfg.getMasterUrl());

        assertSame(cfg, cfg.withMasterUrl("test"));

        assertEquals("test", cfg.getMasterUrl());
    }

    @Test
    public void testTrustCertificates() {
        assertNull(cfg.getTrustCertificates());

        cfg.setTrustCertificates(true);

        assertTrue(cfg.getTrustCertificates());

        cfg.setTrustCertificates(null);

        assertNull(cfg.getTrustCertificates());

        assertSame(cfg, cfg.withTrustCertificates(false));

        assertFalse(cfg.getTrustCertificates());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
