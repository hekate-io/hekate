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

package io.hekate.spring.bean;

import io.hekate.core.resource.ResourceLoadingException;
import io.hekate.core.resource.ResourceService;
import io.hekate.core.service.ServiceFactory;
import java.io.IOException;
import java.io.InputStream;
import org.springframework.context.ApplicationContext;

class SpringResourceService implements ResourceService {
    private final ApplicationContext ctx;

    public SpringResourceService(ApplicationContext ctx) {
        assert ctx != null : "Application context is null.";

        this.ctx = ctx;
    }

    public static ServiceFactory<ResourceService> factory(ApplicationContext ctx) {
        assert ctx != null : "Application context is null.";

        return new ServiceFactory<ResourceService>() {
            @Override
            public ResourceService createService() {
                return new SpringResourceService(ctx);
            }

            @Override
            public String toString() {
                return SpringResourceService.class.getSimpleName() + "Factory";
            }
        };
    }

    @Override
    public InputStream load(String path) throws ResourceLoadingException {
        try {
            return ctx.getResource(path).getInputStream();
        } catch (IOException e) {
            throw new ResourceLoadingException("Failed to load resource [path=" + path + ']', e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
