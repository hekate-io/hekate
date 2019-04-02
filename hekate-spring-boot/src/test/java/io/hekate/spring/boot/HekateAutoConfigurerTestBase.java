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

package io.hekate.spring.boot;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.After;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class HekateAutoConfigurerTestBase extends HekateTestBase {
    public static final String[] EMPTY_ARGS = new String[0];

    private final List<ConfigurableApplicationContext> allCtx = new ArrayList<>();

    private ConfigurableApplicationContext ctx;

    @After
    public void tearDown() throws Exception {
        allCtx.forEach(ConfigurableApplicationContext::close);

        ctx = null;
    }

    protected void registerAndRefresh(Class<?> annotatedClass) {
        registerAndRefresh(null, annotatedClass);
    }

    protected void registerAndRefresh(String[] args, Class<?> configClass) {
        String[] nullSafeArgs = args == null ? EMPTY_ARGS : Stream.of(args).map(arg -> "--" + arg).toArray(String[]::new);

        assertTrue("Context parameters must use '=' to separate name and value: " + Arrays.toString(args),
            Stream.of(nullSafeArgs).allMatch(arg -> arg.indexOf('=') > 0)
        );

        SpringApplication app = new SpringApplication(configClass);
        app.setBannerMode(Banner.Mode.OFF);

        ctx = app.run(nullSafeArgs);

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

    protected ConfigurableApplicationContext getContext() {
        assertNotNull("Application context not initialized.", ctx);

        return ctx;
    }
}
