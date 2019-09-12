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

package io.hekate.core.resource;

import io.hekate.core.resource.internal.DefaultResourceService;
import io.hekate.core.service.ServiceFactory;
import io.hekate.util.format.ToString;

/**
 * Factory for {@link ResourceService}.
 */
public class ResourceServiceFactory implements ServiceFactory<ResourceService> {
    @Override
    public ResourceService createService() {
        return new DefaultResourceService();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
