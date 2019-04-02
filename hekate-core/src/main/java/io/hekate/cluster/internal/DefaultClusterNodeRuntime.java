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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterNodeRuntime;
import io.hekate.core.internal.util.Jvm;
import io.hekate.util.format.ToString;
import java.io.Serializable;

public class DefaultClusterNodeRuntime implements Serializable, ClusterNodeRuntime {
    private static final long serialVersionUID = 1;

    private static final DefaultClusterNodeRuntime LOCAL_INFO;

    static {
        int cpus = Runtime.getRuntime().availableProcessors();
        long maxMemory = Runtime.getRuntime().maxMemory();
        String jvmName = getProperty("java.vm.name");
        String jvmVendor = getProperty("java.vm.vendor");
        String javaVersion = getProperty("java.version");
        String osName = getProperty("os.name");
        String osVersion = getProperty("os.version");
        String osArch = getProperty("os.arch");
        String pid = Jvm.pid();

        LOCAL_INFO = new DefaultClusterNodeRuntime(
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

    public DefaultClusterNodeRuntime(int cpus, long maxMemory, String osName, String osArch, String osVersion, String jvmVersion,
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

    public static DefaultClusterNodeRuntime getLocalInfo() {
        return LOCAL_INFO;
    }

    @Override
    public int cpus() {
        return cpus;
    }

    @Override
    public long maxMemory() {
        return maxMemory;
    }

    @Override
    public String osName() {
        return osName;
    }

    @Override
    public String osArch() {
        return osArch;
    }

    @Override
    public String osVersion() {
        return osVersion;
    }

    @Override
    public String jvmVersion() {
        return jvmVersion;
    }

    @Override
    public String jvmName() {
        return jvmName;
    }

    @Override
    public String jvmVendor() {
        return jvmVendor;
    }

    @Override
    public String pid() {
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
        return ToString.format(ClusterNodeRuntime.class, this);
    }
}
