package io.hekate.core.inject;

/**
 * Resolver of property placeholders in String values.
 */
public interface PlaceholderResolver {
    /**
     * Replaces property placeholders with real values in the provided String value.
     *
     * @param value Value to resolve.
     *
     * @return Resolved value or possibly the original value itself (in case if implementation doesn't support resolution of placeholders
     * or if string doesn't have any placeholders).
     */
    String resolvePlaceholders(String value);
}
