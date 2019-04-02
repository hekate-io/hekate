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

package io.hekate.core.jmx;

import io.hekate.core.HekateException;

/**
 * Signals an error within the {@link JmxService}.
 */
public class JmxServiceException extends HekateException {
    private static final long serialVersionUID = 1693961642160664317L;

    /**
     * Constructs a new instance with the specified error message.
     *
     * @param message Error message.
     */
    public JmxServiceException(String message) {
        super(message);
    }

    /**
     * Constructs a new instance with the specified error message and cause.
     *
     * @param message Error message.
     * @param cause Cause.
     */
    public JmxServiceException(String message, Throwable cause) {
        super(message, cause);
    }
}
