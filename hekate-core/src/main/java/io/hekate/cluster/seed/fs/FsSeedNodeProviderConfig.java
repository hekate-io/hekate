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

package io.hekate.cluster.seed.fs;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.util.format.ToString;
import java.io.File;

/**
 * Configuration for {@link FsSeedNodeProvider}.
 *
 * @see FsSeedNodeProvider#FsSeedNodeProvider(FsSeedNodeProviderConfig)
 */
public class FsSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setCleanupInterval(long)}. */
    public static final long DEFAULT_CLEANUP_INTERVAL = 60 * 1000;

    private long cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    private File workDir;

    /**
     * Returns the time interval in milliseconds between stale node cleanup runs (see {@link #setCleanupInterval(long)}).
     *
     * @return Time interval in milliseconds.
     */
    public long getCleanupInterval() {
        return cleanupInterval;
    }

    /**
     * Sets the time interval in milliseconds between stale node cleanup runs.
     *
     * <p>Default value of this parameter is {@value #DEFAULT_CLEANUP_INTERVAL}.</p>
     *
     * <p>
     * For more details please see the documentation of {@link SeedNodeProvider}.
     * </p>
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @see SeedNodeProvider#cleanupInterval()
     */
    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    /**
     * Fluent-style version of {@link #setCleanupInterval(long)}.
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public FsSeedNodeProviderConfig withCleanupInterval(long cleanupInterval) {
        setCleanupInterval(cleanupInterval);

        return this;
    }

    /**
     * Returns the work directory for seed node provider to store its files (see {@link #setWorkDir(File)}).
     *
     * @return Work directory.
     */
    public File getWorkDir() {
        return workDir;
    }

    /**
     * Sets the work directory for seed node provider to store its files. If directory doesn't exist then it will be automatically created
     * during the seed node provider initialization.
     *
     * <p>
     * This parameter is mandatory and doesn't have a default value.
     * </p>
     *
     * @param workDir Work directory.
     */
    public void setWorkDir(File workDir) {
        this.workDir = workDir;
    }

    /**
     * Fluent-style version of {@link #setWorkDir(File)}.
     *
     * @param workDir Work directory.
     *
     * @return This instance.
     */
    public FsSeedNodeProviderConfig withWorkDir(File workDir) {
        setWorkDir(workDir);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
