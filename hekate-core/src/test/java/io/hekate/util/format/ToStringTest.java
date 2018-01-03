/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.util.format;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ToStringTest extends HekateTestBase {
    private static class TestFormatter implements ToStringFormat.Formatter {
        @Override
        public String format(Object val) {
            return "test-" + val;
        }
    }

    private static class TestNullFormatter implements ToStringFormat.Formatter {
        @Override
        public String format(Object val) {
            return null;
        }
    }

    public static class EmptyClass {
        // No-op.
    }

    public static class ClassA {
        private final String a1;

        private final int a2;

        private final int aCamelCase;

        @ToStringIgnore
        private final String ignore;

        @ToStringFormat(TestFormatter.class)
        private final String customFormat;

        @ToStringFormat(TestNullFormatter.class)
        private String alwaysNull = "alwaysNull";

        public ClassA(String a1, int a2, int aCamelCase, String ignore, String customFormat) {
            this.a1 = a1;
            this.a2 = a2;
            this.aCamelCase = aCamelCase;
            this.ignore = ignore;
            this.customFormat = customFormat;
        }

        public String getF1() {
            return a1;
        }

        public int getF2() {
            return a2;
        }

        public int getCamelCase() {
            return aCamelCase;
        }

        public String getIgnore() {
            return ignore;
        }

        public String getCustomFormat() {
            return customFormat;
        }

        public String getAlwaysNull() {
            return alwaysNull;
        }

        public void setAlwaysNull(String alwaysNull) {
            this.alwaysNull = alwaysNull;
        }
    }

    public static class ClassB extends ClassA {
        private final String b1;

        private final int b2;

        public ClassB(String a1, int a2, int aCamelCase, String ignore, String b1, int b2, String customFormat) {
            super(a1, a2, aCamelCase, ignore, customFormat);

            this.b1 = b1;
            this.b2 = b2;
        }

        public String getB1() {
            return b1;
        }

        public int getB2() {
            return b2;
        }
    }

    public static class ClassC {
        private final String[] c1;

        private final String[][] c2;

        public ClassC(String[] c1, String[][] c2) {
            this.c1 = c1;
            this.c2 = c2;
        }

        public String[] getC1() {
            return c1;
        }

        public String[][] getC2() {
            return c2;
        }
    }

    @Test
    public void testNull() throws Exception {
        assertEquals("null", ToString.format(null));
        assertEquals("null", ToString.format(ClassA.class, null));
    }

    @Test
    public void testObject() throws Exception {
        ClassA objA = new ClassA("AAA", 100, 1, "IGNORE", "ccc");
        ClassB objB = new ClassB("AAA", 100, 1, "IGNORE", "BBB", 200, "ccc");
        EmptyClass emptyObj = new EmptyClass();

        assertEquals("ClassA[a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc]", ToString.format(objA));
        assertEquals("ClassB[a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc, b1=BBB, b2=200]", ToString.format(objB));
        assertEquals("EmptyClass", ToString.format(emptyObj));
    }

    @Test
    public void testObjectWithAlias() throws Exception {
        ClassA objA = new ClassA("AAA", 100, 1, "IGNORE", "ccc");
        ClassB objB = new ClassB("AAA", 100, 1, "IGNORE", "BBB", 200, "ccc");
        EmptyClass emptyObj = new EmptyClass();

        assertEquals("ClassB[a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc]", ToString.format(ClassB.class, objA));
        assertEquals("ClassA[a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc]", ToString.format(objA));

        assertEquals("ClassA[a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc, b1=BBB, b2=200]", ToString.format(ClassA.class, objB));
        assertEquals("ClassB[a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc, b1=BBB, b2=200]", ToString.format(objB));

        assertEquals("ClassB", ToString.format(ClassB.class, emptyObj));
        assertEquals("EmptyClass", ToString.format(emptyObj));
    }

    @Test
    public void testArray() throws Exception {
        String[] arr1 = {"A", "B", "C"};
        String[][] arr2 = {{"A", "B", "C"}, {"D", "E", "F"}};

        ClassC obj = new ClassC(arr1, arr2);

        assertEquals("ClassC[c1=[A, B, C], c2=[[A, B, C], [D, E, F]]]", ToString.format(obj));
    }

    @Test
    public void testPropertiesOnly() throws Exception {
        ClassA objA = new ClassA("AAA", 100, 1, "IGNORE", "ccc");
        ClassB objB = new ClassB("AAA", 100, 1, "IGNORE", "BBB", 200, "ccc");
        EmptyClass emptyObj = new EmptyClass();

        assertEquals("a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc", ToString.formatProperties(objA));
        assertEquals("a1=AAA, a2=100, a-camel-case=1, custom-format=test-ccc, b1=BBB, b2=200", ToString.formatProperties(objB));
        assertEquals("", ToString.formatProperties(emptyObj));
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(ToString.class);
    }
}
