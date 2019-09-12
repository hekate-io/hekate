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

import io.hekate.HekateTestBase;
import java.util.Hashtable;
import javax.management.ObjectName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class JmxUtilsTest extends HekateTestBase {
    @Test
    public void testClusterAndNodeName() throws Exception {
        ObjectName name = new ObjectName("foo.bar", "type", getClass().getSimpleName());

        assertEquals(name, JmxUtils.jmxName("foo.bar", getClass()));
    }

    @Test
    public void testClusterNameOnly() throws Exception {
        ObjectName name = new ObjectName("foo.bar", "type", getClass().getSimpleName());

        assertEquals(name, JmxUtils.jmxName("foo.bar", getClass()));
    }

    @Test
    public void testNameAttribute() throws Exception {
        Hashtable<String, String> attrs = new Hashtable<>();

        attrs.put("name", "test-name");
        attrs.put("type", getClass().getSimpleName());

        ObjectName name = new ObjectName("foo.bar", attrs);

        assertEquals(name, JmxUtils.jmxName("foo.bar", getClass(), "test-name"));
    }

    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(JmxUtils.class);
    }
}
