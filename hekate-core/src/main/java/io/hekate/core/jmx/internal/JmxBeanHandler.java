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

package io.hekate.core.jmx.internal;

import javax.management.MBeanRegistration;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;

class JmxBeanHandler extends StandardMBean {
    private final Object bean;

    @SuppressWarnings("unchecked")
    public JmxBeanHandler(Object bean, Class<?> type, boolean mxBean) {
        super(bean, (Class<Object>)type, mxBean);

        this.bean = bean;
    }

    @Override
    public ObjectName preRegister(MBeanServer server, ObjectName name) throws Exception {
        ObjectName superName = super.preRegister(server, name);

        if (bean instanceof MBeanRegistration) {
            return ((MBeanRegistration)bean).preRegister(server, superName);
        }

        return superName;
    }

    @Override
    public void postRegister(Boolean done) {
        super.postRegister(done);

        if (bean instanceof MBeanRegistration) {
            ((MBeanRegistration)bean).postRegister(done);
        }
    }

    @Override
    public void preDeregister() throws Exception {
        super.preDeregister();

        if (bean instanceof MBeanRegistration) {
            ((MBeanRegistration)bean).preDeregister();
        }
    }

    @Override
    public void postDeregister() {
        super.postDeregister();

        if (bean instanceof MBeanRegistration) {
            ((MBeanRegistration)bean).postDeregister();
        }
    }
}
