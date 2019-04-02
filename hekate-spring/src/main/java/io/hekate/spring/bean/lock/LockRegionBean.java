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

package io.hekate.spring.bean.lock;

import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.spring.bean.HekateBaseBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Imports {@link LockRegion} into a Spring context.
 */
public class LockRegionBean extends HekateBaseBean<LockRegion> {
    private String region;

    @Override
    public LockRegion getObject() throws Exception {
        return getSource().locks().region(getRegion());
    }

    @Override
    public Class<LockRegion> getObjectType() {
        return LockRegion.class;
    }

    /**
     * Returns the lock region name (see {@link #setRegion(String)}).
     *
     * @return Lock region name.
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the name of a lock region that should imported into a Spring context.
     *
     * @param region Lock region name.
     *
     * @see LockRegionConfig#setName(String)
     */
    @Required
    public void setRegion(String region) {
        this.region = region;
    }
}
