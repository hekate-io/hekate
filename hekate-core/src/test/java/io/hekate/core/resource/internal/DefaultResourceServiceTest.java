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

package io.hekate.core.resource.internal;

import io.hekate.HekateTestBase;
import io.hekate.core.resource.ResourceLoadingException;
import java.io.File;
import java.io.InputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DefaultResourceServiceTest extends HekateTestBase {
    private final DefaultResourceService service = new DefaultResourceService();

    @Test
    public void testSuccess() throws Exception {
        File[] files = new File("./").listFiles();

        assertNotNull(files);

        int checked = 0;

        for (File file : files) {
            if (file.isFile()) {
                String path = "file:///" + file.getCanonicalPath();

                say("Testing path: " + path);

                try (InputStream stream = service.load(path)) {
                    assertNotNull(stream);
                    assertEquals(file.length(), stream.available());

                    checked++;
                }
            }
        }

        assertTrue(checked > 0);
    }

    @Test
    public void testToString() {
        assertEquals(DefaultResourceService.class.getSimpleName(), service.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullPath() throws Exception {
        service.load(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testEmptyPath() throws Exception {
        service.load("");
    }

    @Test(expected = ResourceLoadingException.class)
    public void testInvalidUrl() throws Exception {
        service.load("invalid-url");
    }

    @Test(expected = ResourceLoadingException.class)
    public void testInvalidPath() throws Exception {
        service.load("file:///there-is-no-such-file.ever");
    }
}
