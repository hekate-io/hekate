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

package io.hekate.coordinate;

/**
 * Provided information about the coordination process.
 *
 * @see CoordinationService#process(String)
 */
public interface CoordinationProcess {
    /**
     * Returns the process name (see {@link CoordinationProcessConfig#setName(String)}).
     *
     * @return Process name.
     */
    String name();

    /**
     * Returns the initial coordination future of this process. The returned future object will be completed once this coordination
     * processes gets executed for the very first time.
     *
     * @return Initial coordination future.
     */
    CoordinationFuture future();

    /**
     * Returns the handler of this process (see {@link CoordinationProcessConfig#setHandler(CoordinationHandler)}).
     *
     * @return Handler.
     */
    CoordinationHandler handler();
}
