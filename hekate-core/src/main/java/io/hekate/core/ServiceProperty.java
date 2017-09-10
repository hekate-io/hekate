package io.hekate.core;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.io.Serializable;

/**
 * Property of {@link ServiceInfo}.
 *
 * @param <T> Property type.
 */
public final class ServiceProperty<T> implements Serializable {
    /**
     * Service property type.
     *
     * @see ServiceProperty#type()
     */
    public enum Type {
        /** For {@link String} type. */
        STRING,

        /** For {@link Integer} type. */
        INTEGER,

        /** For {@link Long} type. */
        LONG,

        /** For {@link Boolean} type. */
        BOOLEAN
    }

    private static final long serialVersionUID = 1L;

    private final Type type;

    private final String name;

    private final T value;

    private ServiceProperty(Type type, String name, T value) {
        ArgAssert.notNull(type, "Property type");
        ArgAssert.notNull(name, "Property name");
        ArgAssert.notNull(value, "Property value");

        this.type = type;
        this.name = name;
        this.value = value;
    }

    /**
     * Constructs a new property of {@link Type#STRING} type.
     *
     * @param name Property name.
     * @param value Property value.
     *
     * @return New property.
     */
    public static ServiceProperty<String> forString(String name, String value) {
        return new ServiceProperty<>(Type.STRING, name, value);
    }

    /**
     * Constructs a new property of {@link Type#INTEGER} type.
     *
     * @param name Property name.
     * @param value Property value.
     *
     * @return New property.
     */
    public static ServiceProperty<Integer> forInteger(String name, int value) {
        return new ServiceProperty<>(Type.INTEGER, name, value);
    }

    /**
     * Constructs a new property of {@link Type#LONG} type.
     *
     * @param name Property name.
     * @param value Property value.
     *
     * @return New property.
     */
    public static ServiceProperty<Long> forLong(String name, long value) {
        return new ServiceProperty<>(Type.LONG, name, value);
    }

    /**
     * Constructs a new property of {@link Type#BOOLEAN} type.
     *
     * @param name Property name.
     * @param value Property value.
     *
     * @return New property.
     */
    public static ServiceProperty<Boolean> forBoolean(String name, boolean value) {
        return new ServiceProperty<>(Type.BOOLEAN, name, value);
    }

    /**
     * Returns the property type.
     *
     * @return Property type.
     */
    public Type type() {
        return type;
    }

    /**
     * Returns the property name.
     *
     * @return Property name.
     */
    public String name() {
        return name;
    }

    /**
     * Returns the property value.
     *
     * @return Property value.
     */
    public T value() {
        return value;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
