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

package io.hekate.metrics;

import io.hekate.util.format.ToString;
import java.util.regex.Pattern;

/**
 * Implementation of {@link MetricFilter} that uses a regular expression to match against a {@link Metric#name()}.
 */
public class MetricRegexFilter implements MetricFilter {
    private final Pattern pattern;

    /**
     * Constructs new instance with the specified regular expression.
     *
     * @param regex Regular expression.
     */
    public MetricRegexFilter(String regex) {
        pattern = Pattern.compile(regex);
    }

    @Override
    public boolean accept(Metric metric) {
        return pattern.matcher(metric.name()).matches();
    }

    /**
     * Returns the regular expression of this filter.
     *
     * @return Regular expression.
     */
    public String getPattern() {
        return pattern.pattern();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
