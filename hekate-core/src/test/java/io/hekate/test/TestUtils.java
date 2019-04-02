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

package io.hekate.test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public final class TestUtils {
    private TestUtils() {
        // No-op.
    }

    public static File createTempDir() throws IOException {
        return Files.createTempDirectory("hekate_test").toFile();
    }

    public static void deleteDir(File dir) {
        File[] files = dir.listFiles();

        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDir(f);
                }

                f.delete();
            }
        }

        dir.delete();
    }
}
