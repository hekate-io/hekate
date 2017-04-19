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

package io.hekate.spring.bean;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import io.hekate.inject.InjectionService;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HekateSpringBootstrapTest extends HekateTestBase {
    @Test
    public void test() throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("hekate.xml");

        Hekate hekate = null;

        for (int i = 0; i < 1; i++) {
            hekate = ctx.getBean("hekate", Hekate.class);

            assertSame(Hekate.State.UP, hekate.getState());

            InjectionService injection = hekate.get(InjectionService.class);

            assertTrue(injection.toString(), injection.toString().startsWith(SpringInjectionService.class.getSimpleName()));

            ctx.refresh();

            assertSame(Hekate.State.DOWN, hekate.getState());
        }

        ctx.close();

        assertSame(Hekate.State.DOWN, hekate.getState());
    }
}
