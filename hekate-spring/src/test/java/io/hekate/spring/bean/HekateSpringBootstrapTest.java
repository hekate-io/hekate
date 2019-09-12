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

package io.hekate.spring.bean;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.inject.HekateInject;
import io.hekate.core.inject.InjectionService;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.resource.ResourceService;
import java.io.InputStream;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StreamUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class HekateSpringBootstrapTest extends HekateTestBase {
    @HekateInject
    private static class InjectTest {
        @Autowired
        private Hekate node;

        public Hekate getNode() {
            return node;
        }
    }

    @Test
    public void test() throws Exception {
        ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext("hekate.xml");

        Hekate node = null;

        for (int i = 0; i < 3; i++) {
            node = ctx.getBean("hekate", Hekate.class);

            assertSame(Hekate.State.UP, node.state());

            verifyInjection(node);
            verifyResource(node);

            ctx.refresh();

            assertSame(Hekate.State.DOWN, node.state());
        }

        ctx.close();

        assertSame(Hekate.State.DOWN, node.state());
    }

    private void verifyResource(Hekate hekate) throws Exception {
        ResourceService resource = hekate.get(ResourceService.class);

        assertEquals(resource.toString(), SpringResourceService.class.getSimpleName(), resource.toString());

        try (InputStream in = resource.load("hekate.xml")) {
            assertNotNull(in);

            String str = StreamUtils.copyToString(in, Utils.UTF_8);

            assertTrue(str, str.contains("<bean name=\"hekate\""));
        }
    }

    private void verifyInjection(Hekate hekate) {
        InjectionService injection = hekate.get(InjectionService.class);

        assertEquals(injection.toString(), SpringInjectionService.class.getSimpleName(), injection.toString());

        InjectTest inject = new InjectTest();

        injection.inject(inject);

        assertNotNull(inject.getNode());
    }
}
