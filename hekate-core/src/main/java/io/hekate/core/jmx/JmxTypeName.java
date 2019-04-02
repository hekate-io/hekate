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

package io.hekate.core.jmx;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.management.MXBean;
import javax.management.ObjectName;

/**
 * Provides a custom value for the {@code 'type'} attribute of an {@link ObjectName}.
 *
 * <p>
 * This annotation can be placed on an {@link MXBean}-annotated interface of a JMX object that gets registered to the {@link JmxService}.
 * If such annotation is present then its {@link #value()} will override the default (auto-generated) value of the {@code 'type'} attribute
 * of that JMX objects's {@link ObjectName}.
 * </p>
 *
 * @see JmxService
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JmxTypeName {
    /**
     * Returns the value for the {@link ObjectName}'s {@code 'type'} attribute.
     *
     * @return Value for the {@link ObjectName}'s {@code 'type'} attribute.
     */
    String value();
}
