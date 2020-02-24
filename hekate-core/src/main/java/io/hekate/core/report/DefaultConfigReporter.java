/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.core.report;

import static io.hekate.core.internal.util.Utils.NL;

/**
 * Default implementation of {@link ConfigReporter interface}.
 *
 * <p>
 * This class uses an internal buffer to accumulate reported values. Resulting report can be obtained via {@link #report()} method.
 * </p>
 */
public class DefaultConfigReporter implements ConfigReporter {
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
        buf.append(indent).append(name).append(':').append(NL);

        return new DefaultConfigReporter(indent + INDENT_STEP, buf);
    }
}
