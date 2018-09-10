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
