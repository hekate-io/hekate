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

package io.hekate.core.inject;

import io.hekate.core.Hekate;
import io.hekate.core.service.Service;

/**
 * Dependency injection service.
 *
 *
 * <h2>Overview</h2>
 * <p>
 * This interface represents the abstraction layer of dependency injection capabilities that are provided by an underlying
 * <a href="https://en.wikipedia.org/wiki/Inversion_of_control" target="_blank">IoC</a> container like
 * <a href="http://projects.spring.io/spring-framework" target="_blank">Spring Framework</a>. Typically this service is used for injecting
 * dependencies into objects that were received from some external sources (f.e. deserialized from a socket).
 * </p>
 *
 * <h2>Accessing the Service</h2>
 * <p>
 * {@link InjectionService} is available only if {@link Hekate} instance was constructed via framework-specific bootstrap class
 * (like <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>). Availability of this service can
 * be checked via {@link Hekate#has(Class)} method. If service is available then it can be accessed via {@link Hekate#get(Class)} as in the
 * example below:
 * ${source: core/inject/InjectionServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>Enabling Injection</h2>
 * <p>
 * In order to enable injection the {@link HekateInject} annotation must be added to a class who's dependencies should be injected.
 * Classes that do not have this annotation will be ignored by the {@link InjectionService}.
 * </p>
 *
 * <h2>Injectable Components</h2>
 * <p>
 * The following components can can be injected:
 * </p>
 * <ul>
 * <li>{@link Hekate} instance that manages this service.</li>
 * <li>All {@link Hekate#services() services} that are registered within the {@link Hekate} instance</li>
 * <li>Any other components that are managed by the underlying IoC framework (f.e. beans from
 * <a href="http://projects.spring.io/spring-framework" target="_blank">Spring Framework</a> application context)</li>
 * </ul>
 *
 * <p>
 * <b>Note:</b> this service performs injection of fields/properties only. Other extended capabilities (like lifecycle management) that can
 * be provided by an underlying IoC container are not supported.
 * </p>
 */
public interface InjectionService extends Service {
    /**
     * Injects dependencies into the specified object.
     *
     * @param obj Dependent object.
     */
    void inject(Object obj);
}
