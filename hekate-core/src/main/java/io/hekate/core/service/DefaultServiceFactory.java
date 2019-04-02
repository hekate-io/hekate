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

package io.hekate.core.service;

import io.hekate.core.HekateBootstrap;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;

/**
 * Specifies the default factory for a {@link Service} interface.
 *
 * <p>
 * This annotation can be placed on a {@link Service} interface in order to specify which factory should be used to construct a new
 * service instance by default.
 * </p>
 *
 * <p>
 * Default instantiation takes place when some service {@code A} {@link DependencyContext#require(Class) depends} on some other service
 * {@code B} and there is no factory for service {@code B} being {@link HekateBootstrap#setServices(List) registered}. In such case the
 * presence of this annotation will be checked on the service {@code B}'s interface and if it exists then service will be constructed via
 * the annotated factory.
 * </p>
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface DefaultServiceFactory {
    /**
     * Service factory class. Must be instantiable via reflections (i.e. must have a default no-arg public constructor).
     *
     * @return Service factory class.
     */
    Class<? extends ServiceFactory<?>> value();
}
