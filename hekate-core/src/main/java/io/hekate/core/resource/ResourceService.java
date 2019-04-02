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

import io.hekate.core.Hekate;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.io.InputStream;
import java.net.URL;

/**
 * Resources loading service.
 *
 * <h2>Overview</h2>
 * <p>
 * This service provides an abstraction layer for accessing file system resources. Functionality of this service depends on the underlying
 * runtime. In the simplest case, if {@link Hekate} node is directly constructed by the Java application then it will use {@link URL}-based
 * resources loading. Alternatively, if {@link Hekate} node is managed by the <a href="http://projects.spring.io/spring-framework"
 * target="_blank">Spring Framework</a> then it will utilize the framework's resource loading capabilities.
 * </p>
 *
 * <h2>Accessing the Service</h2>
 * <p>
 * {@link ResourceService} can be accessed via {@link Hekate#get(Class)} method as in the example below:
 * ${source: core/resource/ResourceServiceJavadocTest.java#access}
 * </p>
 */
@DefaultServiceFactory(ResourceServiceFactory.class)
public interface ResourceService extends Service {
    /**
     * Loads a resource from the specified location.
     *
     * @param path Resource path.
     *
     * @return Input stream.
     *
     * @throws ResourceLoadingException Signals that resource couldn't be loaded.
     */
    InputStream load(String path) throws ResourceLoadingException;
}
