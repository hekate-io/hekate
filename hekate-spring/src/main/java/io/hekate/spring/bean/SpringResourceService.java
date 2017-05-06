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
