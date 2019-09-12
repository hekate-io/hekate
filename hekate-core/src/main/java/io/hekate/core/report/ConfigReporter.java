package io.hekate.core.report;

import java.util.function.Consumer;

/**
 * Helper class to build configuration reports.
 */
public interface ConfigReporter {
    /**
     * Add the specified value to this report.
     *
     * @param key Key.
     * @param value Value.
     */
    void value(String key, Object value);

    /**
     * Creates a new sub-section of this report.
     *
     * @param name Section name.
     *
     * @return Section.
     */
    ConfigReporter section(String name);

    /**
     * Applies the given consumer to a sub-section of this report.
     *
     * @param name Section name (see {@link #section(String)}).
     * @param report Consumer of sub-section reporter.
     */
    default void section(String name, Consumer<ConfigReporter> report) {
        report.accept(section(name));
    }
}
