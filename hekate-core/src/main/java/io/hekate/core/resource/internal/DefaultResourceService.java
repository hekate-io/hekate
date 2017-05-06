package io.hekate.core.resource.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.resource.ResourceLoadingException;
import io.hekate.core.resource.ResourceService;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * {@link URL}-based implementation of the {@link ResourceService} interface.
 */
public class DefaultResourceService implements ResourceService {
    @Override
    public InputStream load(String path) throws ResourceLoadingException {
        ArgAssert.notNull(path, "Resource path");
        ArgAssert.isFalse(path.isEmpty(), "Resource path is empty.");

        try {
            URL url = new URL(path);

            try (InputStream in = url.openStream()) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();

                byte[] buf = new byte[4 * 1024];

                for (int read = in.read(buf); read > 0; read = in.read(buf)) {
                    out.write(buf, 0, read);
                }

                return new ByteArrayInputStream(out.toByteArray());
            }
        } catch (IOException e) {
            throw new ResourceLoadingException("Failed to load resource [path=" + path + ']', e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
