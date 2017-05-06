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
 * <h2>Accessing service</h2>
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
