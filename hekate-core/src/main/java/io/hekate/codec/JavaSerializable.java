package io.hekate.codec;

import java.io.Serializable;

/**
 * Marker interface for classes that should use the Java default serialization policy.
 *
 * <p>
 * Some {@link Codec}s can use this interface in order to enforce JDK default serialization on objects that implement this interface and
 * bypass some internal optimizations.
 * </p>
 */
public interface JavaSerializable extends Serializable {
    // No-op.
}
