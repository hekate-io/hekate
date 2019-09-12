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

import io.hekate.core.Hekate;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;

/**
 * Base class for beans that provide support for importing {@link Hekate} components and services into a Spring context.
 *
 * @param <T> Imported bean type.
 */
public abstract class HekateBaseBean<T> implements FactoryBean<T>, BeanFactoryAware {
    private Hekate source;

    private BeanFactory beanFactory;

    /**
     * Returns {@code true}.
     *
     * @return {@code true}
     */
    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * Return the {@link Hekate} instance that will be used as a source of imported components and services (see {@link
     * #setSource(Hekate)}).
     *
     * @return {@link Hekate} instance that will be used as a source of imported components and services.
     */
    public Hekate getSource() {
        if (source == null) {
            source = beanFactory.getBean(Hekate.class);
        }

        return source;
    }

    /**
     * Sets the {@link Hekate} instance that will be used as a source of imported components and services. If not specified then it will be
     * auto-discovered via {@link BeanFactory#getBean(Class)}.
     *
     * @param source {@link Hekate} instance that will be used as a source of imported components and services.
     */
    public void setSource(Hekate source) {
        this.source = source;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }
}
