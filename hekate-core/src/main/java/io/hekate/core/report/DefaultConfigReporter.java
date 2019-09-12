package io.hekate.core.report;

/**
 * Default implementation of {@link ConfigReporter interface}.
 *
 * <p>
 * This class uses an internal buffer to accumulate reported values. Resulting report can be obtained via {@link #report()} method.
 * </p>
 */
public class DefaultConfigReporter implements ConfigReporter {
    /** New line separator. */
    private static final String NL = System.lineSeparator();

    /** Indentation offset. */
    private static final String INDENT_STEP = "  ";

    /** Indent. */
    private final String indent;

    /** Report buffer. */
    private final StringBuilder buf;

    /**
     * Constructs a new instance.
     */
    public DefaultConfigReporter() {
        this.indent = INDENT_STEP;
        this.buf = new StringBuilder(NL);
    }

    /**
     * Internal constructor for sub-sections.
     *
     * @param indent Indent.
     * @param buf Buffer.
     */
    private DefaultConfigReporter(String indent, StringBuilder buf) {
        this.indent = indent;
        this.buf = buf;
    }

    /**
     * Returns a configuration report for the specified subject.
     *
     * @param subject Subject.
     *
     * @return Report.
     */
    public static String report(ConfigReportSupport subject) {
        DefaultConfigReporter reporter = new DefaultConfigReporter();

        subject.report(reporter);

        return reporter.report();
    }

    /**
     * Returns report.
     *
     * @return Report.
     */
    public String report() {
        return buf.toString();
    }

    @Override
    public void value(String key, Object value) {
        if (value != null) {
            if (value instanceof ConfigReportSupport) {
                ((ConfigReportSupport)value).report(section(key));
            } else {
                buf.append(indent).append(key).append(": ").append(value).append(NL);
            }
        }
    }

    @Override
    public ConfigReporter section(String name) {
        buf.append(indent).append(name).append(NL);

        return new DefaultConfigReporter(indent + INDENT_STEP, buf);
    }
}
