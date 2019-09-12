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

package io.hekate.codec;

import java.io.IOException;

/**
 * Functional interface for decoding data from binary format.
 *
 * @param <T> Type of the result of the function.
 */
@FunctionalInterface
public interface DecodeFunction<T> {
    /**
     * Decodes object from the provided reader.
     *
     * @param in Data reader.
     *
     * @return Decoded object.
     *
     * @throws IOException If object couldn't be decoded.
     */
    T decode(DataReader in) throws IOException;
}
