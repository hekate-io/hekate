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
