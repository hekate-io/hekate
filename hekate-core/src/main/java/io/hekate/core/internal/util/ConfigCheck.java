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

package io.hekate.core.internal.util;

import io.hekate.core.HekateConfigurationException;
import java.util.Set;
import java.util.regex.Pattern;

public final class ConfigCheck {
    private static final Pattern SYS_NAME = Pattern.compile("^[a-z0-9]+([-.][a-z0-9]+)*$", Pattern.CASE_INSENSITIVE);

    private final String component;

    private ConfigCheck(String component) {
        this.component = component;
    }

    public static ConfigCheck get(Class<?> component) {
        return new ConfigCheck(component.getSimpleName());
    }

    public void that(boolean check, String msg) throws HekateConfigurationException {
        if (!check) {
            throw new HekateConfigurationException(component + ": " + msg);
        }
    }

    public HekateConfigurationException fail(Throwable cause) {
        throw new HekateConfigurationException(component + ": " + cause.toString(), cause);
    }

    public HekateConfigurationException fail(String msg) {
        throw new HekateConfigurationException(component + ": " + msg);
    }

    public void validSysName(String value, String component) {
        if (value != null) {
            String trimmed = value.trim();

            if (!trimmed.isEmpty() && !SYS_NAME.matcher(trimmed).matches()) {
                throw fail(component + " can contain only alpha-numeric characters and non-repeatable dots/hyphens "
                    + "[value=" + trimmed + ']');
            }
        }
    }

    public void range(int value, int from, int to, String component) {
        that(value >= from && value <= to, component + " must be within the " + from + ".." + to + " range.");
    }

    public void positive(int value, String component) {
        greater(value, 0, component);
    }

    public void positive(long value, String component) {
        greater(value, 0, component);
    }

    public void nonNegative(int value, String component) {
        greaterOrEquals(value, 0, component);
    }

    public void greater(long value, long than, String component) {
        that(value > than, component + " must be greater than " + than + " [value=" + value + ']');
    }

    public void greater(int value, int than, String component) {
        that(value > than, component + " must be greater than " + than + " [value=" + value + ']');
    }

    public void greaterOrEquals(int value, int than, String component) {
        that(value >= than, component + " must be greater than or equals to " + than + " [value=" + value + ']');
    }

    public void unique(Object what, Set<?> where, String component) {
        that(!where.contains(what), "duplicated " + component + " [value=" + what + ']');
    }

    public void isFalse(boolean value, String msg) throws HekateConfigurationException {
        that(!value, msg);
    }

    public void isTrue(boolean value, String msg) throws HekateConfigurationException {
        that(value, msg);
    }

    public void notNull(Object value, String component) throws HekateConfigurationException {
        notNull(value, component, "must be not null");
    }

    public void notNull(Object value, String component, String details) throws HekateConfigurationException {
        that(value != null, component + ' ' + details + ".");
    }

    public void notEmpty(String value, String component) throws HekateConfigurationException {
        notNull(value, component);

        that(!value.trim().isEmpty(), component + " must be a non-empty string.");
    }

    public void isPowerOfTwo(int n, String component) throws HekateConfigurationException {
        that(Utils.isPowerOfTwo(n), component + " must be a power of two [value=" + n + ']');
    }
}
