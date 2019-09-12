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

package io.hekate.spring.boot.core.jmx;

import io.hekate.core.jmx.JmxService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateJmxServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class JmxTestConfig extends HekateTestConfigBase {
        // No-op.
    }

    @Test
    public void testJmxEnabled() {
        registerAndRefresh(new String[]{"hekate.jmx.enable=true"}, JmxTestConfig.class);

        assertTrue(getNode().has(JmxService.class));
    }

    @Test
    public void testJmxDisabled() {
        registerAndRefresh(JmxTestConfig.class);

        assertFalse(getNode().has(JmxService.class));
    }
}
