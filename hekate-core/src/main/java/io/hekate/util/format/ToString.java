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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

/**
 * Utility for building {@link Object#toString()} methods.
 */
public final class ToString {
    private static class FieldFormat {
        private static final Pattern FIELD_NAME_PATTERN = Pattern.compile("([^_A-Z])([A-Z])");

        private final String nameEq;

        private final Field field;

        private final ToStringFormat.Formatter formatter;

        public FieldFormat(Field field) {
            this.nameEq = formatName(field.getName()) + '=';
            this.field = field;

            ToStringFormat format = field.getAnnotation(ToStringFormat.class);

            if (format == null) {
                this.formatter = tryDefaultFormatter(field.getType());
            } else {
                Class<? extends ToStringFormat.Formatter> type = format.value();

                try {
                    ToStringFormat.Formatter customFormat = null;

                    try {
                        for (Constructor<?> c : type.getDeclaredConstructors()) {
                            if (c != null && c.getParameterCount() == 0) {
                                c.setAccessible(true);

                                customFormat = (ToStringFormat.Formatter)c.newInstance();
                            }
                        }
                    } catch (SecurityException | InvocationTargetException e) {
                        // No-op.
                    }

                    if (customFormat == null) {
                        customFormat = type.getConstructor().newInstance();
                    }

                    this.formatter = customFormat;
                } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                    throw new IllegalStateException("Failed to instantiate formatter [type=" + type.getName() + ']', e);
                }
            }
        }

        public boolean format(Object obj, StringBuilder buf, boolean prependSeparator) throws IllegalAccessException {
            Object val = field.get(obj);

            if (formatter != null) {
                // Try to use custom formatter.
                String fmtVal = formatter.format(val);

                if (fmtVal != null) {
                    if (prependSeparator) {
                        buf.append(", ");
                    }

                    buf.append(nameEq).append(fmtVal);

                    return true;
                }

                return false;
            } else if (val != null) {
                // Use default formatting (only if value is not null).
                if (prependSeparator) {
                    buf.append(", ");
                }

                buf.append(nameEq);

                if (val.getClass().isArray()) {
                    formatArray(buf, val);
                } else if (val instanceof Class<?>) {
                    Class<?> clazz = (Class<?>)val;

                    buf.append(clazz.getName());
                } else {
                    buf.append(val);
                }

                return true;
            } else {
                return false;
            }
        }

        private ToStringFormat.Formatter tryDefaultFormatter(Class<?> type) {
            if (Optional.class.isAssignableFrom(type)) {
                return val -> val != null ? ((Optional<?>)val).map(String::valueOf).orElse(null) : null;
            } else if (OptionalInt.class.isAssignableFrom(type)) {
                return val -> {
                    if (val != null) {
                        OptionalInt opt = (OptionalInt)val;

                        return opt.isPresent() ? String.valueOf(opt.getAsInt()) : null;
                    } else {
                        return null;
                    }
                };
            } else if (OptionalLong.class.isAssignableFrom(type)) {
                return val -> {
                    if (val != null) {
                        OptionalLong opt = (OptionalLong)val;

                        return opt.isPresent() ? String.valueOf(opt.getAsLong()) : null;
                    } else {
                        return null;
                    }
                };
            } else if (OptionalDouble.class.isAssignableFrom(type)) {
                return val -> {
                    if (val != null) {
                        OptionalDouble opt = (OptionalDouble)val;

                        return opt.isPresent() ? String.valueOf(opt.getAsDouble()) : null;
                    } else {
                        return null;
                    }
                };
            } else {
                return null;
            }
        }

        private static void formatArray(StringBuilder buf, Object val) {
            buf.append('[');

            for (int i = 0, size = Array.getLength(val); i < size; i++) {
                if (i > 0) {
                    buf.append(", ");
                }

                Object arrVal = Array.get(val, i);

                if (arrVal.getClass().isArray()) {
                    formatArray(buf, arrVal);
                } else {
                    buf.append(arrVal);
                }
            }

            buf.append(']');
        }

        private static String formatName(String name) {
            return FIELD_NAME_PATTERN.matcher(name).replaceAll("$1-$2").toLowerCase(Locale.US);
        }
    }

    private static class ClassFormat {
        private final String name;

        private final int approxSize;

        private final List<FieldFormat> fields;

        public ClassFormat(Class<?> type) {
            this.name = type.getSimpleName();

            int approxSize = 0;

            List<FieldFormat> fields = new ArrayList<>();

            do {
                Field[] declaredFields = type.getDeclaredFields();

                if (declaredFields.length > 0) {
                    List<FieldFormat> typeFields = new ArrayList<>(declaredFields.length);

                    for (Field field : declaredFields) {
                        if (!field.isSynthetic()) {
                            int mod = field.getModifiers();

                            if (!Modifier.isStatic(mod) && field.getAnnotation(ToStringIgnore.class) == null) {
                                field.setAccessible(true);

                                typeFields.add(new FieldFormat(field));

                                // This is really approximate.
                                approxSize += field.getName().length() * 2;
                            }
                        }
                    }

                    if (!typeFields.isEmpty()) {
                        fields.addAll(0, typeFields);
                    }
                }

                type = type.getSuperclass();
            }
            while (type != null && type != Object.class);

            this.fields = fields;
            this.approxSize = approxSize;
        }

        public String format(Class<?> alias, boolean propertiesOnly, Object obj) throws IllegalAccessException {
            StringBuilder buf;

            if (propertiesOnly) {
                if (approxSize == 0) {
                    // There are no fields in this class.
                    return "";
                } else {
                    buf = new StringBuilder(approxSize);
                }
            } else {
                String realName;
                int realApproxSize;

                if (alias == null) {
                    realName = name;
                } else {
                    realName = alias.getSimpleName();
                }

                realApproxSize = approxSize + realName.length() + 2/* this is for '[' ']' */;

                buf = new StringBuilder(realApproxSize);

                buf.append(realName);
            }

            if (!fields.isEmpty()) {
                if (!propertiesOnly) {
                    buf.append('[');
                }

                boolean appendSeparator = false;

                for (FieldFormat field : fields) {
                    boolean formatted = field.format(obj, buf, appendSeparator);

                    if (formatted) {
                        appendSeparator = true;
                    }
                }

                if (!propertiesOnly) {
                    buf.append(']');
                }
            }

            return buf.toString();
        }
    }

    private static final ReentrantReadWriteLock.ReadLock READ_LOCK;

    private static final ReentrantReadWriteLock.WriteLock WRITE_LOCK;

    private static final Map<Class<?>, ClassFormat> FORMATS = new HashMap<>();

    static {
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        READ_LOCK = lock.readLock();
        WRITE_LOCK = lock.writeLock();
    }

    private ToString() {
        // No-op.
    }

    /**
     * Formats the specified object to string including the object's type information.
     * <p>
     * Example: {@code MyObj[fieldA=1, fieldB=ZZZ]}
     * </p>
     *
     * @param obj Object to be formatted.
     *
     * @return String representation of the specified object.
     */
    public static String format(Object obj) {
        return doFormat(null, false, obj);
    }

    /**
     * Formats the specified object to string by prefixing it with the specified type name instead of the original object type.
     * <p>
     * Example: {@code MyTypeAlias[fieldA=1, fieldB=ZZZ]}
     * </p>
     *
     * @param alias Type who's name should be added to the formatted string.
     * @param obj Object to be formatted.
     *
     * @return String representation of the specified object.
     */
    public static String format(Class<?> alias, Object obj) {
        return doFormat(alias, false, obj);
    }

    /**
     * Returns the comma-separated list of object's fields and values.
     *
     * @param obj Object to be formatted.
     *
     * @return Comma-separated list of object's fields and values.
     */
    public static String formatProperties(Object obj) {
        return doFormat(null, true, obj);
    }

    private static String doFormat(Class<?> alias, boolean propertiesOnly, Object obj) {
        if (obj == null) {
            return "null";
        }

        Class<?> type = obj.getClass();

        ClassFormat format;

        READ_LOCK.lock();

        try {
            format = FORMATS.get(type);
        } finally {
            READ_LOCK.unlock();
        }

        if (format == null) {
            WRITE_LOCK.lock();

            try {
                format = FORMATS.computeIfAbsent(type, ClassFormat::new);

            } finally {
                WRITE_LOCK.unlock();
            }
        }

        try {
            return format.format(alias, propertiesOnly, obj);
        } catch (IllegalAccessException e) {
            return "TO_STRING_FAILURE: " + e;
        }
    }
}
