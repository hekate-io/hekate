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

package io.hekate.task;

import java.io.Serializable;
import java.util.Collection;

/**
 * Task that can be applied to an argument in order to produce some result.
 *
 * <p>
 * Please see the documentation of {@link TaskService} for more details of distributed tasks processing.
 * </p>
 *
 * @param <T> Argument type.
 * @param <V> Result type.
 *
 * @see TaskService#applyAll(Collection, ApplicableTask)
 */
@FunctionalInterface
public interface ApplicableTask<T, V> extends Serializable {
    /**
     * Applies this task to the specified argument.
     *
     * @param arg Argument.
     *
     * @return Result.
     *
     * @throws TaskException Task execution error.
     */
    V apply(T arg) throws TaskException;
}
