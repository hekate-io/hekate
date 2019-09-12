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

import io.hekate.core.jmx.JmxTypeName;
import java.io.File;
import javax.management.MXBean;

/**
 * JMX interface for {@link FsSeedNodeProvider}.
 */
@MXBean
@JmxTypeName("FsSeedNodeProvider")
public interface FsSeedNodeProviderJmx {
    /**
     * Returns the string representation of {@link FsSeedNodeProviderConfig#setWorkDir(File)}.
     *
     * @return String representation of {@link FsSeedNodeProviderConfig#setWorkDir(File)}
     */
    String getWorkDir();

    /**
     * Returns the string representation of {@link FsSeedNodeProviderConfig#setCleanupInterval(long)}.
     *
     * @return String representation of {@link FsSeedNodeProviderConfig#setCleanupInterval(long)}
     */
    long getCleanupInterval();
}
