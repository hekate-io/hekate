/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.util.trace;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import java.util.HashMap;
import java.util.Map;

/**
 * Information about a {@link Traceable} object.
 */
public final class TraceInfo {
    /** See {@link #name()}. */
    private final String name;

    /** See {@link #tags()}. */
    private Map<String, Object> tags;

    private TraceInfo(String name) {
        ArgAssert.notNull(name, "Name");

        this.name = name;
    }

    /**
     * Tries to extract {@link TraceInfo} from the specified object.
     *
     * <p>
     * If object is an instance of {@link Traceable} interface then its {@link Traceable#traceInfo()} will be returned; otherwise this
     * method returns {@code null}.
     * </p>
     *
     * @param obj Object.
     *
     * @return Tracing information if the specified object is on {@link Traceable} type; otherwise {@code null}.
     */
    public static TraceInfo extract(Object obj) {
        if (obj instanceof Traceable) {
            return ((Traceable)obj).traceInfo();
        } else {
            return null;
        }
    }

    /**
     * Constructs a new instance.
     *
     * @param name Trace name (see {@link #name()}).
     *
     * @return New instance of {@link TraceInfo}.
     */
    public static TraceInfo of(String name) {
        return new TraceInfo(name);
    }

    /**
     * Name of this trace.
     *
     * @return Name of this trace.
     */
    public String name() {
        return name;
    }

    /**
     * Tags of this trace.
     *
     * @return Tags of this trace.
     */
    public Map<String, Object> tags() {
        return tags;
    }

    /**
     * Adds a new tag to this trace.
     *
     * @param name Tag name.
     * @param value Tag value.
     *
     * @return This instance.
     */
    public TraceInfo withTag(String name, Object value) {
        if (name != null && value != null) {
            if (tags == null) {
                tags = new HashMap<>();
            }

            tags.put(name, value);
        }

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
