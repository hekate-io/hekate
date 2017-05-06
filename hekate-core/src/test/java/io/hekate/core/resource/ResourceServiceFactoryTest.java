package io.hekate.core.resource;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ResourceServiceFactoryTest extends HekateTestBase {
    private final ResourceServiceFactory factory = new ResourceServiceFactory();

    @Test
    public void testCreate() {
        assertNotNull(factory.createService());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(factory), factory.toString());
    }
}
