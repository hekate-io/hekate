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

package io.hekate.spring.boot.messaging;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingService;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Provides support for {@link MessagingChannel}s autowiring.
 *
 * <p>
 * This annotation can be placed on any {@link Autowired autowire}-capable elements (fields, properties, parameters, etc) of application
 * beans in order to inject {@link MessagingChannel} by its {@link MessagingChannelConfig#setName(String) name}.
 * </p>
 *
 * <p>
 * Below is the example of how this annotation can be used.
 * </p>
 *
 * <p>
 * 1) Define a bean that will use {@link InjectChannel} annotation to inject {@link MessagingChannel} into its field.
 * ${source:messaging/MessagingInjectionJavadocTest.java#channel_bean}
 * 2) Define a Spring Boot application that will provide channel configuration.
 * ${source:messaging/MessagingInjectionJavadocTest.java#app}
 * </p>
 *
 * @see HekateMessagingServiceConfigurer
 */
@Autowired
@Qualifier
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE})
public @interface InjectChannel {
    /**
     * Specifies the {@link MessagingChannelConfig#setName(String) name} of a {@link MessagingChannel} that should be injected (see {@link
     * MessagingService#channel(String)}).
     *
     * @return Name of a {@link MessagingChannel}.
     */
    String value();
}
