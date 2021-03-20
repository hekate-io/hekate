/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.codec;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.hekate.HekateTestBase;
import java.io.Serializable;
import java.util.List;
import java.util.SortedSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class HekateSerializableClassesTest extends HekateTestBase {
    @Test
    public void testKnownClasses() throws Exception {
        SortedSet<Class<?>> known = HekateSerializableClasses.get();

        for (String name : scanForSerializableClasses()) {
            Class<?> clazz = Class.forName(name);

            assertTrue(clazz.getName(), known.contains(clazz));
        }
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(HekateSerializableClasses.class);
    }

    private List<String> scanForSerializableClasses() {
        return new ClassGraph()
            .whitelistPackages("io.hekate")
            .enableAllInfo()
            .scan()
            .getAllClasses()
            .filter(c -> !c.isInterface() && !c.isAbstract())
            .filter(c -> c.implementsInterface(Serializable.class.getName()))
            .stream().flatMap(c ->
                Stream.concat(Stream.of(c), c.getSubclasses().stream())
            )
            .filter(c -> !c.getClasspathElementFile().getAbsolutePath().contains("test"))
            .map(ClassInfo::getName)
            .collect(Collectors.toList());
    }
}
