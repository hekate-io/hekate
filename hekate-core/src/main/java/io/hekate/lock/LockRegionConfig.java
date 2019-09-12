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

package io.hekate.lock;

import io.hekate.util.format.ToString;

/**
 * Configuration for {@link LockRegion}.
 *
 * <p>
 * Instances of this class can be {@link LockServiceFactory#withRegion(LockRegionConfig) registered} within the {@link LockServiceFactory}
 * in order to make particular lock region available in the {@link LockService}.
 * </p>
 *
 * <p>
 * For more details about locks and regions please see the documentation of the {@link LockService} interface.
 * </p>
 *
 * @see LockServiceFactory#withRegion(LockRegionConfig)
 */
public class LockRegionConfig {
    private String name;

    /**
     * Constructs new instance.
     */
    public LockRegionConfig() {
        // No-op.
    }

    /**
     * Constructs new instance with the specified name.
     *
     * @param name Name (see {@link #setName(String)}).
     */
    public LockRegionConfig(String name) {
        this.name = name;
    }

    /**
     * Returns the lock region name (see {@link #setName(String)}).
     *
     * @return Lock region name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the lock region name. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * <p>
     * This name can be used to obtain reference to {@link LockRegion} via {@link LockService#region(String)}.
     * </p>
     *
     * <p>
     * Value of this parameter is mandatory and must be unique across all regions registered within the {@link LockServiceFactory}.
     * </p>
     *
     * @param name Region name (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Fluent-style version of {@link #setName(String)}.
     *
     * @param name Region name.
     *
     * @return This instance.
     */
    public LockRegionConfig withName(String name) {
        setName(name);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
