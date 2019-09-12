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

package io.hekate.util.format;

import io.hekate.HekateTestBase;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
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
        private final String strVal;

        private final int intVal;

        private final int camelCase;

        @ToStringIgnore
        private final String ignore;

        @ToStringFormat(TestFormatter.class)
        private final String customFormat;

        private final Optional<String> optStr;

        private final OptionalInt optInt;

        private final OptionalLong optLong;

        private final OptionalDouble optDouble;

        @ToStringFormat(TestNullFormatter.class)
        private String alwaysNull = "alwaysNull";

        public ClassA(
            String strVal,
            int intVal,
            int camelCase,
            String ignore,
            String customFormat,
            Optional<String> optStr,
            OptionalInt optInt,
            OptionalLong optLong,
            OptionalDouble optDouble
        ) {
            this.strVal = strVal;
            this.intVal = intVal;
            this.camelCase = camelCase;
            this.ignore = ignore;
            this.customFormat = customFormat;
            this.optStr = optStr;
            this.optInt = optInt;
            this.optLong = optLong;
            this.optDouble = optDouble;
        }

        public String getStrVal() {
            return strVal;
        }

        public int getIntVal() {
            return intVal;
        }

        public int getCamelCase() {
            return camelCase;
        }

        public String getIgnore() {
            return ignore;
        }

        public String getCustomFormat() {
            return customFormat;
        }

        public Optional<String> getOptStr() {
            return optStr;
        }

        public OptionalInt getOptInt() {
            return optInt;
        }

        public OptionalLong getOptLong() {
            return optLong;
        }

        public OptionalDouble getOptDouble() {
            return optDouble;
        }

        public String getAlwaysNull() {
            return alwaysNull;
        }

        public void setAlwaysNull(String alwaysNull) {
            this.alwaysNull = alwaysNull;
        }
    }

    public static class ClassB extends ClassA {
        private final String strVal2;

        private final int intVal2;

        public ClassB(
            String a1,
            int a2,
            int aCamelCase,
            String ignore,
            String strVal2,
            int intVal2,
            String customFormat,
            Optional<String> optStr,
            OptionalInt optInt,
            OptionalLong optLong,
            OptionalDouble optDouble
        ) {
            super(a1, a2, aCamelCase, ignore, customFormat, optStr, optInt, optLong, optDouble);

            this.strVal2 = strVal2;
            this.intVal2 = intVal2;
        }

        public String getStrVal2() {
            return strVal2;
        }

        public int getIntVal2() {
            return intVal2;
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
        ClassA objA = new ClassA(
            "AAA",
            100,
            1,
            "IGNORE",
            "ccc",
            Optional.empty(),
            OptionalInt.empty(),
            OptionalLong.empty(),
            OptionalDouble.empty()
        );

        ClassB objB = new ClassB(
            "AAA",
            100,
            1,
            "IGNORE",
            "BBB",
            200,
            "ccc",
            Optional.empty(),
            OptionalInt.empty(),
            OptionalLong.empty(),
            OptionalDouble.empty()
        );

        EmptyClass emptyObj = new EmptyClass();

        assertEquals(
            "ClassA["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc"
                + "]",
            ToString.format(objA)
        );

        assertEquals(
            "ClassB["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc, "
                + "str-val2=BBB, "
                + "int-val2=200"
                + "]",
            ToString.format(objB)
        );
        assertEquals("EmptyClass", ToString.format(emptyObj));
    }

    @Test
    public void testObjectWithAlias() throws Exception {
        ClassA objA = new ClassA(
            "AAA",
            100,
            1,
            "IGNORE",
            "ccc",
            Optional.empty(),
            OptionalInt.empty(),
            OptionalLong.empty(),
            OptionalDouble.empty()
        );

        ClassB objB = new ClassB(
            "AAA",
            100,
            1,
            "IGNORE",
            "BBB",
            200,
            "ccc",
            Optional.empty(),
            OptionalInt.empty(),
            OptionalLong.empty(),
            OptionalDouble.empty()
        );

        EmptyClass emptyObj = new EmptyClass();

        assertEquals(
            "ClassB["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc"
                + "]",
            ToString.format(ClassB.class, objA)
        );

        assertEquals(
            "ClassA["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc"
                + "]",
            ToString.format(objA)
        );

        assertEquals(
            "ClassA["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc, "
                + "str-val2=BBB, "
                + "int-val2=200"
                + "]",
            ToString.format(ClassA.class, objB)
        );

        assertEquals(
            "ClassB["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc, "
                + "str-val2=BBB, "
                + "int-val2=200"
                + "]",
            ToString.format(objB)
        );

        assertEquals("ClassB", ToString.format(ClassB.class, emptyObj));
        assertEquals("EmptyClass", ToString.format(emptyObj));
    }

    @Test
    public void testOptional() throws Exception {
        ClassA objA = new ClassA(
            "AAA",
            100,
            1,
            "IGNORE",
            "ccc",
            Optional.of("TEST"),
            OptionalInt.of(1050),
            OptionalLong.of(100500),
            OptionalDouble.of(100.500)
        );

        ClassB objB = new ClassB(
            "AAA",
            100,
            1,
            "IGNORE",
            "BBB",
            200,
            "ccc",
            Optional.of("TEST"),
            OptionalInt.of(1050),
            OptionalLong.of(100500),
            OptionalDouble.of(100.500)
        );

        assertEquals(
            "ClassA["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc, "
                + "opt-str=TEST, "
                + "opt-int=1050, "
                + "opt-long=100500, "
                + "opt-double=100.5"
                + "]",
            ToString.format(objA)
        );

        assertEquals(
            "ClassB["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc, "
                + "opt-str=TEST, "
                + "opt-int=1050, "
                + "opt-long=100500, "
                + "opt-double=100.5, "
                + "str-val2=BBB, "
                + "int-val2=200"
                + "]",
            ToString.format(objB)
        );
    }

    @Test
    public void testOptionalNull() throws Exception {
        ClassA objA = new ClassA(
            "AAA",
            100,
            1,
            "IGNORE",
            "ccc",
            null,
            null,
            null,
            null
        );

        assertEquals(
            "ClassA["
                + "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc"
                + "]",
            ToString.format(objA)
        );
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
        ClassA objA = new ClassA(
            "AAA",
            100,
            1,
            "IGNORE",
            "ccc",
            Optional.empty(),
            OptionalInt.empty(),
            OptionalLong.empty(),
            OptionalDouble.empty()
        );

        ClassB objB = new ClassB(
            "AAA",
            100,
            1,
            "IGNORE",
            "BBB",
            200,
            "ccc",
            Optional.empty(),
            OptionalInt.empty(),
            OptionalLong.empty(),
            OptionalDouble.empty()
        );

        EmptyClass emptyObj = new EmptyClass();

        assertEquals(
            "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc",
            ToString.formatProperties(objA)
        );

        assertEquals(
            "str-val=AAA, "
                + "int-val=100, "
                + "camel-case=1, "
                + "custom-format=test-ccc, "
                + "str-val2=BBB, "
                + "int-val2=200",
            ToString.formatProperties(objB)
        );

        assertEquals("", ToString.formatProperties(emptyObj));
    }

    @Test
    public void testFieldOfClassType() {
        class TestClass {
            private final Class<TestClass> type1;

            public TestClass(Class<TestClass> type1) {
                this.type1 = type1;
            }
        }

        assertEquals(
            "type1=" + TestClass.class.getName(),
            ToString.formatProperties(new TestClass(TestClass.class))
        );
    }

    @Test
    public void testNullValue() {
        class TestClass {
            private final Object val;

            public TestClass(Object val) {
                this.val = val;
            }
        }

        assertEquals("", ToString.formatProperties(new TestClass(null)));
    }

    @Test
    public void testValidUtilityClass() throws Exception {
        assertValidUtilityClass(ToString.class);
    }
}
