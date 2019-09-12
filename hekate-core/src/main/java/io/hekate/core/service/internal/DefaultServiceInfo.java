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

package io.hekate.core.service.internal;

import io.hekate.core.ServiceInfo;
import io.hekate.core.ServiceProperty;
import io.hekate.util.format.ToStringIgnore;
import java.io.Serializable;
import java.util.Map;

public class DefaultServiceInfo implements ServiceInfo, Serializable {
    private static final long serialVersionUID = 1;

    @ToStringIgnore
    private final String type;

    private final Map<String, ServiceProperty<?>> props;

    public DefaultServiceInfo(String type, Map<String, ServiceProperty<?>> props) {
        assert type != null : "Service type is null.";
        assert props != null : "Service properties are null.";

        this.type = type;
        this.props = props;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public Map<String, ServiceProperty<?>> properties() {
        return props;
    }

    @Override
    public ServiceProperty<?> property(String name) {
        return props.get(name);
    }

    @Override
    public String toString() {
        return props.toString();
    }
}
