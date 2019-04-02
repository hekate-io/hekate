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

package io.hekate.spring.boot.lock;

import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Provides support for {@link DistributedLock}s autowiring.
 *
 * <p>
 * This annotation can be placed on any {@link Autowired autowire}-capable elements (fields, properties, parameters, etc) of application
 * beans in order to inject {@link DistributedLock} by its {@link LockRegionConfig#setName(String) region} and
 * {@link LockRegion#get(String) lock} names.
 * </p>
 *
 * <p>
 * Below is the example of how this annotation can be used.
 * </p>
 *
 * <p>
 * 1) Define a bean that will use {@link InjectLock} annotation to inject {@link DistributedLock} into its field.
 * ${source:lock/LockInjectionJavadocTest.java#lock_bean}
 * 2) Define a Spring Boot application that will provide region configuration for that lock.
 * ${source:lock/LockInjectionJavadocTest.java#lock_app}
 * </p>
 *
 * @see HekateLockServiceConfigurer
 * @see LockRegion#get(String)
 */
@Autowired
@Qualifier
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
public @interface InjectLock {
    /**
     * Specifies the name of a {@link DistributedLock} that should be used to get the lock (see {@link LockRegion#get(String)}).
     *
     * @return Name of a {@link DistributedLock}.
     */
    String name();

    /**
     * Specifies the {@link LockRegionConfig#setName(String) name} of a {@link LockRegion} that should be used to get the lock (see {@link
     * LockService#region(String)}).
     *
     * @return Name of a {@link LockRegion}.
     */
    String region();
}
