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

package io.hekate.core.jmx.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.jmx.JmxTypeName;
import java.util.Hashtable;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * JMX utilities.
 */
final class JmxUtils {
    private JmxUtils() {
        // No-op.
    }

    /**
     * Constructs a new JMX object name.
     *
     * @param domain JMX domain.
     * @param type JMX interface.
     *
     * @return Object name.
     *
     * @throws MalformedObjectNameException Signals on malformed object name.
     */
    public static ObjectName jmxName(String domain, Class<?> type) throws MalformedObjectNameException {
        return jmxName(domain, type, null);
    }

    /**
     * Constructs a new JMX object name.
     *
     * @param domain JMX domain.
     * @param type JMX interface.
     * @param name Value for the {@link ObjectName}'s {@code name} attribute.
     *
     * @return Object name.
     *
     * @throws MalformedObjectNameException Signals on malformed object name.
     */
    public static ObjectName jmxName(String domain, Class<?> type, String name) throws MalformedObjectNameException {
        ArgAssert.notEmpty(domain, "Domain");
        ArgAssert.notNull(type, "Type");

        String mayBeName = Utils.nullOrTrim(name);

        Hashtable<String, String> attrs = new Hashtable<>();

        if (type.isAnnotationPresent(JmxTypeName.class)) {
            attrs.put("type", type.getAnnotation(JmxTypeName.class).value());
        } else {
            attrs.put("type", type.getSimpleName());
        }

        if (mayBeName != null) {
            attrs.put("name", mayBeName);
        }

        return new ObjectName(domain.trim(), attrs);
    }
}
