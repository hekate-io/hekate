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

package io.hekate.spring.boot.coordination;

import io.hekate.coordinate.CoordinationContext;
import io.hekate.coordinate.CoordinationHandler;
import io.hekate.coordinate.CoordinationProcessConfig;
import io.hekate.coordinate.CoordinationRequest;
import io.hekate.coordinate.CoordinationService;
import io.hekate.coordinate.CoordinatorContext;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertNotNull;

public class HekateCoordinationServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class CoordinationTestConfig extends HekateTestConfigBase {
        @Autowired
        private CoordinationService coordinationService;

        @Bean
        public CoordinationProcessConfig process1() {
            return new CoordinationProcessConfig().withName("test.process").withHandler(new CoordinationHandler() {
                @Override
                public void prepare(CoordinationContext ctx) {
                    // No-op.
                }

                @Override
                public void coordinate(CoordinatorContext ctx) {
                    ctx.complete();
                }

                @Override
                public void process(CoordinationRequest request, CoordinationContext ctx) {
                    // No-op.
                }
            });
        }
    }

    @Test
    public void testProcesses() throws Exception {
        registerAndRefresh(CoordinationTestConfig.class);

        assertNotNull(get("coordinationService", CoordinationService.class));
        assertNotNull(get(CoordinationTestConfig.class).coordinationService);
        assertNotNull(getNode().coordination().process("test.process").future().get());
    }
}
