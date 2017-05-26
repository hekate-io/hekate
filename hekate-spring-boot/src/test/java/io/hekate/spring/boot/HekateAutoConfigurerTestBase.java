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

package io.hekate.spring.boot;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.springframework.boot.test.util.EnvironmentTestUtils;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

import static org.junit.Assert.assertNotNull;

public abstract class HekateAutoConfigurerTestBase extends HekateTestBase {
    private final List<AnnotationConfigApplicationContext> allCtx = new ArrayList<>();

    private AnnotationConfigApplicationContext ctx;

    @After
    public void tearDown() throws Exception {
        allCtx.forEach(AbstractApplicationContext::close);

        ctx = null;
    }

    protected void registerAndRefresh(Class<?>... annotatedClasses) {
        registerAndRefresh(null, annotatedClasses);
    }

    protected void registerAndRefresh(String[] env, Class<?>... annotatedClasses) {
        ctx = new AnnotationConfigApplicationContext();

        ctx.register(annotatedClasses);

        if (env != null && env.length > 0) {
            EnvironmentTestUtils.addEnvironment(ctx, env);
        }

        ctx.refresh();

        allCtx.add(ctx);
    }

    protected <T> T autowire(T obj) {
        getContext().getAutowireCapableBeanFactory().autowireBean(obj);

        return obj;
    }

    protected Hekate getNode() {
        return get(Hekate.class);
    }

    protected <T> T get(Class<T> type) {
        return getContext().getBean(type);
    }

    protected <T> T get(String name, Class<T> type) {
        return getContext().getBean(name, type);
    }

    protected AnnotationConfigApplicationContext getContext() {
        assertNotNull("Application context not initialized.", ctx);

        return ctx;
    }
}
