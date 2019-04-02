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

package io.hekate.spring.bean.network;

import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.spring.bean.HekateBaseBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Imports {@link NetworkConnector} into a Spring context.
 */
public class NetworkConnectorBean extends HekateBaseBean<NetworkConnector<?>> {
    private String protocol;

    @Override
    public NetworkConnector<?> getObject() throws Exception {
        return getSource().network().connector(protocol);
    }

    @Override
    public Class<NetworkConnector> getObjectType() {
        return NetworkConnector.class;
    }

    /**
     * Returns the protocol name.
     *
     * @return Protocol name.
     */
    public String getProtocol() {
        return protocol;
    }

    /**
     * Sets the protocol name.
     *
     * @param protocol Protocol name.
     *
     * @see NetworkConnectorConfig#setProtocol(String)
     */
    @Required
    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
}
