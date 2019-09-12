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

import javax.management.MBeanServer;
import javax.management.MXBean;

/**
 * Adaptor interface that can be implemented by any object that indirectly provides JMX support.
 *
 * <p>
 * If object that implement this interface is passed to the {@link JmxService#register(Object)} method then the real management object that
 * will be registered to the {@link MBeanServer} will be an object that is returned by the {@link #jmx()} method.
 * </p>
 *
 * @param <T> {@link MXBean} interface type.
 *
 * @see JmxService#register(Object, String)
 */
public interface JmxSupport<T> {
    /**
     * Creates a JMX object.
     *
     * @return JMX object.
     */
    T jmx();
}
