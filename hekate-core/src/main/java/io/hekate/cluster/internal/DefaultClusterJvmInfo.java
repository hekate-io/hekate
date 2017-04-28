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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterJvmInfo;
import io.hekate.core.internal.util.Utils;
import io.hekate.util.format.ToString;
import java.io.Serializable;

public class DefaultClusterJvmInfo implements Serializable, ClusterJvmInfo {
    private static final long serialVersionUID = 1L;

    private static final DefaultClusterJvmInfo LOCAL_INFO;

    static {
        int cpus = Runtime.getRuntime().availableProcessors();
        long maxMemory = Runtime.getRuntime().maxMemory();
        String jvmName = getProperty("java.vm.name");
        String jvmVendor = getProperty("java.vm.vendor");
        String javaVersion = getProperty("java.version");
        String osName = getProperty("os.name");
        String osVersion = getProperty("os.version");
        String osArch = getProperty("os.arch");
        String pid = Utils.getPid();

        LOCAL_INFO = new DefaultClusterJvmInfo(
            cpus,
            maxMemory,
            osName,
            osArch,
            osVersion,
            javaVersion,
            jvmName,
            jvmVendor,
            pid
        );

    }

    private final String pid;

    private final int cpus;

    private final long maxMemory;

    private final String osName;

    private final String osVersion;

    private final String osArch;

    private final String jvmVersion;

    private final String jvmName;

    private final String jvmVendor;

    public DefaultClusterJvmInfo(int cpus, long maxMemory, String osName, String osArch, String osVersion, String jvmVersion,
        String jvmName, String jvmVendor, String pid) {
        this.cpus = cpus;
        this.maxMemory = maxMemory;
        this.osName = osName;
        this.osArch = osArch;
        this.osVersion = osVersion;
        this.jvmVersion = jvmVersion;
        this.jvmName = jvmName;
        this.jvmVendor = jvmVendor;
        this.pid = pid;
    }

    public static DefaultClusterJvmInfo getLocalInfo() {
        return LOCAL_INFO;
    }

    @Override
    public int getCpus() {
        return cpus;
    }

    @Override
    public long getMaxMemory() {
        return maxMemory;
    }

    @Override
    public String getOsName() {
        return osName;
    }

    @Override
    public String getOsArch() {
        return osArch;
    }

    @Override
    public String getOsVersion() {
        return osVersion;
    }

    @Override
    public String getJvmVersion() {
        return jvmVersion;
    }

    @Override
    public String getJvmName() {
        return jvmName;
    }

    @Override
    public String getJvmVendor() {
        return jvmVendor;
    }

    @Override
    public String getPid() {
        return pid;
    }

    private static String getProperty(String name) {
        try {
            String val = System.getProperty(name);

            return val == null ? "" : val;
        } catch (SecurityException e) {
            return "";
        }
    }

    @Override
    public String toString() {
        return ToString.format(ClusterJvmInfo.class, this);
    }
}
