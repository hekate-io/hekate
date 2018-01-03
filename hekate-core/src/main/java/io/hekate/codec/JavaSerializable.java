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

package io.hekate.codec;

import java.io.Serializable;

/**
 * Marker interface for classes that should use the Java default serialization policy.
 *
 * <p>
 * Some {@link Codec}s can use this interface in order to enforce JDK default serialization on objects that implement this interface and
 * bypass some internal optimizations.
 * </p>
 */
public interface JavaSerializable extends Serializable {
    // No-op.
}
