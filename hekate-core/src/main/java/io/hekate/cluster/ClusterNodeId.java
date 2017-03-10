/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.cluster;

import io.hekate.util.UuidBase;

/**
 * Universally unique identifier of {@link ClusterNode}.
 *
 * @see ClusterNode#getId()
 */
public class ClusterNodeId extends UuidBase<ClusterNodeId> {
    private static final long serialVersionUID = 1;

    /**
     * Constructs new random identifier.
     */
    public ClusterNodeId() {
        // No-op.
    }

    /**
     * Constructs new instance from the specified higher/lower bits.
     *
     * @param hiBits Higher bits (see {@link #getHiBits()}).
     * @param loBits Lower bits (see {@link #getLoBits()}).
     */
    public ClusterNodeId(long hiBits, long loBits) {
        super(hiBits, loBits);
    }

    /**
     * Creates new identifier from the specified string.
     *
     * <p>
     * Only strings that were produced by the {@link #toString()} method can be parsed.
     * </p>
     *
     * @param s String (see {@link #toString()}).
     */
    public ClusterNodeId(String s) {
        super(s);
    }
}
