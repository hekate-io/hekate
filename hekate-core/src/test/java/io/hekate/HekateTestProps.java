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

package io.hekate;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public final class HekateTestProps {
    private static final String DEFAULT_FILE_NAME = "test.properties";

    private static final String USER_FILE_NAME = "my_test.properties";

    private static final Properties PROPERTIES;

    static {
        File base = new File("").getAbsoluteFile();

        Properties props = new Properties();

        Properties defaultProps = loadProperties(base, DEFAULT_FILE_NAME);

        if (defaultProps != null) {
            defaultProps.stringPropertyNames().forEach(name -> props.setProperty(name, defaultProps.getProperty(name)));
        }

        Properties userProps = loadProperties(base, USER_FILE_NAME);

        if (userProps != null) {
            userProps.stringPropertyNames().forEach(name -> props.setProperty(name, userProps.getProperty(name)));
        }

        if (props.isEmpty()) {
            throw new IllegalStateException("Failed to load test properties." + System.lineSeparator()
                + System.lineSeparator()
                + "     CAUSE: File '" + USER_FILE_NAME + "' couldn't be found starting at '" + base.getAbsolutePath() + "'"
                + " location and upwards the directory tree."
                + System.lineSeparator()
                + System.lineSeparator()
                + "  SOLUTION: Copy '<PROJECT_DIR>/" + USER_FILE_NAME + ".example' file to '<PROJECT_DIR>/" + USER_FILE_NAME + '\''
                + System.lineSeparator()
                + System.lineSeparator());
        }

        PROPERTIES = props;
    }

    private HekateTestProps() {
        // No-op.
    }

    public static boolean is(String key) {
        return Boolean.parseBoolean(get(key));
    }

    public static String get(String key) {
        String val = doGet(key);

        assertNotNull("Test property not configured [key=" + key + ']', val);
        assertFalse("Test property has empty value  [key=" + key + ']', val.trim().isEmpty());

        return val.trim();
    }

    private static String doGet(String key) {
        String val = System.getenv(key);

        if (val != null) {
            return val;
        }

        val = System.getProperty(key);

        if (val != null) {
            return val;
        }

        return PROPERTIES.getProperty(key);
    }

    private static Properties loadProperties(File baseDir, String fileName) {
        Properties props = null;

        File dir = baseDir;

        do {
            File propsFile = new File(dir.getAbsoluteFile(), fileName);

            if (propsFile.isFile()) {
                props = new Properties();

                try (FileInputStream in = new FileInputStream(propsFile)) {
                    props.load(in);
                } catch (IOException e) {
                    throw new IllegalStateException("Failed to read test properties [file=" + propsFile.getAbsolutePath() + ']', e);
                }

                break;
            } else {
                dir = dir.getParentFile();
            }
        } while (dir != null);

        return props;
    }
}
