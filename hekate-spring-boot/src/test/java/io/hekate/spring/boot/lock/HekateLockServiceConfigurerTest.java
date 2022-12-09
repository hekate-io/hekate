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

package io.hekate.spring.boot.lock;

import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HekateLockServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class LockTestConfig extends HekateTestConfigBase {
        private static class InnerBean {
            @InjectLockRegion("test1")
            private LockRegion region2;

            @InjectLock(region = "test1", name = "lock2")
            private DistributedLock lock2;

            @InjectLockRegion("test1")
            private LockRegion region1;

            @InjectLock(region = "test1", name = "lock1")
            private DistributedLock lock1;
        }

        @Bean
        public InnerBean innerBean() {
            return new InnerBean();
        }

        @Bean
        public LockRegion region1(LockService lockService) {
            return lockService.region("test1");
        }

        @Bean
        public LockRegion region2(LockService lockService) {
            return lockService.region("test2");
        }

        @Bean
        public LockRegionConfig region1Config() {
            return new LockRegionConfig().withName("test1");
        }

        @Bean
        public LockRegionConfig region2Config() {
            return new LockRegionConfig().withName("test2");
        }
    }

    @Test
    public void testRegions() {
        registerAndRefresh(LockTestConfig.class);

        assertNotNull(get("lockService", LockService.class));

        assertNotNull(get(LockTestConfig.InnerBean.class).region1);
        assertNotNull(get(LockTestConfig.InnerBean.class).region2);
        assertNotNull(get(LockTestConfig.InnerBean.class).lock1);
        assertNotNull(get(LockTestConfig.InnerBean.class).lock2);

        assertEquals("lock1", get(LockTestConfig.InnerBean.class).lock1.name());
        assertEquals("lock2", get(LockTestConfig.InnerBean.class).lock2.name());

        assertNotNull(getNode().locks().region("test1"));
        assertNotNull(getNode().locks().region("test2"));

        class TestAutowire {
            @Autowired
            private LockService lockService;

            @Autowired
            @Qualifier("region1")
            private LockRegion region1;

            @Autowired
            @Qualifier("region2")
            private LockRegion region2;
        }

        assertNotNull(autowire(new TestAutowire()).region1);
        assertNotNull(autowire(new TestAutowire()).region2);
        assertNotNull(autowire(new TestAutowire()).lockService);
    }
}
