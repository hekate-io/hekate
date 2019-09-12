package io.hekate.core.report;

/**
 * Base interface for components that support configuration reports via {@link ConfigReporter}.
 */
public interface ConfigReportSupport {
    /**
     * Report configuration.
     *
     * @param report Reporter.
     */
    void report(ConfigReporter report);
}
