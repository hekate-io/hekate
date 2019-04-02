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

package io.hekate.spring.bean.rpc;

import io.hekate.spring.bean.HekateBaseBean;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.util.StringUtils;

/**
 * Imports RPC client proxy into the Spring context.
 *
 * @param <T> RPC service interface.
 */
public class RpcClientBean<T> extends HekateBaseBean<T> {
    private Class<T> rpcInterface;

    private String tag;

    @Override
    public T getObject() throws Exception {
        if (StringUtils.hasText(getTag())) {
            return getSource().rpc().clientFor(getRpcInterface(), getTag()).build();
        } else {
            return getSource().rpc().clientFor(getRpcInterface()).build();
        }
    }

    @Override
    public Class<?> getObjectType() {
        return getRpcInterface();
    }

    /**
     * Returns the RPC service interface (see {@link #setRpcInterface(Class)}).
     *
     * @return RPC service interface.
     */
    public Class<T> getRpcInterface() {
        return rpcInterface;
    }

    /**
     * Sets the RPC service interface.
     *
     * @param rpcInterface RPC service interface.
     */
    @Required
    public void setRpcInterface(Class<T> rpcInterface) {
        this.rpcInterface = rpcInterface;
    }

    /**
     * Returns the RPC service tag.
     *
     * @return RPC service tag.
     */
    public String getTag() {
        return tag;
    }

    /**
     * Sets the RPC service tag.
     *
     * @param tag RPC service tag.
     */
    public void setTag(String tag) {
        this.tag = tag;
    }
}
