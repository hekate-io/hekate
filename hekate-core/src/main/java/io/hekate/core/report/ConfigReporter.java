/*
 * Copyright 2022 The Hekate Project
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
