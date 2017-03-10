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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;

/**
 * System information.
 *
 * @see Hekate#getSysInfo()
 * @see ClusterNode#getSysInfo()
 */
public interface SystemInfo {
    /**
     * Returns the number of processors available to the Java virtual machine.
     *
     * @return Number of processors available to the Java virtual machine.
     *
     * @see Runtime#availableProcessors()
     */
    int getCpus();

    /**
     * Returns the maximum amount of memory as provided by {@link Runtime#maxMemory()}.
     *
     * @return Maximum amount of memory.
     *
     * @see Runtime#maxMemory()
     */
    long getMaxMemory();

    /**
     * Returns the operating system name ('os.name' {@link System#getProperty(String) system property}).
     *
     * @return Operating system name or an empty string if such information is not available.
     */
    String getOsName();

    /**
     * Returns the operating system architecture ('os.arch' {@link System#getProperty(String) system property}).
     *
     * @return Operating system architecture or an empty string if such information is not available.
     */
    String getOsArch();

    /**
     * Returns the operating system version ('os.version' {@link System#getProperty(String) system property}).
     *
     * @return Operating system version or an empty string if such information is not available.
     */
    String getOsVersion();

    /**
     * Returns the Java runtime environment version ('java.version' {@link System#getProperty(String) system property}).
     *
     * @return Java runtime environment version or an empty string if such information is not available.
     */
    String getJvmVersion();

    /**
     * Returns the Java virtual machine implementation name ('java.vm.name' {@link System#getProperty(String) system property}).
     *
     * @return Java virtual machine implementation name or an empty string if such information is not available.
     */
    String getJvmName();

    /**
     * Returns the Java virtual machine implementation vendor ('java.vm.vendor' {@link System#getProperty(String) system property}).
     *
     * @return Java virtual machine implementation name or an empty string if such information is not available.
     */
    String getJvmVendor();

    /**
     * Returns <a href="https://en.wikipedia.org/wiki/Process_identifier" target="_blank">PID</a> of a Java virtual machine system process.
     *
     * @return <a href="https://en.wikipedia.org/wiki/Process_identifier" target="_blank">PID</a> of a Java virtual machine system process.
     */
    String getPid();
}
