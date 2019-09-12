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

package io.hekate.cluster.seed.jclouds;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class BasicCredentialsSupplierTest extends HekateTestBase {
    private final BasicCredentialsSupplier supplier = new BasicCredentialsSupplier();

    @Test
    public void testIdentity() {
        assertNull(supplier.getIdentity());

        supplier.setIdentity("test1");

        assertEquals("test1", supplier.getIdentity());

        supplier.setIdentity(null);

        assertNull(supplier.getIdentity());

        assertSame(supplier, supplier.withIdentity("test2"));

        assertEquals("test2", supplier.getIdentity());

        assertEquals("test2", supplier.get().identity);
    }

    @Test
    public void testCredential() {
        assertNull(supplier.getCredential());

        supplier.setCredential("test1");

        assertEquals("test1", supplier.getCredential());

        supplier.setCredential(null);

        assertNull(supplier.getCredential());

        assertSame(supplier, supplier.withCredential("test2"));

        assertEquals("test2", supplier.getCredential());

        assertEquals("test2", supplier.get().credential);
    }
}
