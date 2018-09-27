package io.hekate.cluster.seed.jclouds;

import java.util.Properties;
import org.jclouds.Constants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CloudPropertiesBasePlainTest extends CloudPropertiesBaseTestBase {
    @Test
    public void testEmpty() {
        CloudPropertiesBase config = createConfig();

        assertTrue(config.buildBaseProperties().isEmpty());
    }

    @Test
    public void testProperties() {
        CloudPropertiesBase config = createConfig()
            .withConnectTimeout(1050)
            .withSoTimeout(100500);

        Properties props = config.buildBaseProperties();

        assertEquals("1050", props.getProperty(Constants.PROPERTY_CONNECTION_TIMEOUT));
        assertEquals("100500", props.getProperty(Constants.PROPERTY_SO_TIMEOUT));
    }

    @Override
    protected CloudPropertiesBase createConfig() {
        return new CloudPropertiesBase() {
            // No-op.
        };
    }
}
