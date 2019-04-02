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

package io.hekate.core;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Build version.
 */
public final class HekateVersion {
    private static final String PROD_NAME;

    private static final String VER_SHORT;

    private static final String VER_FULL;

    private static final String DATE;

    private static final String REV;

    private static final String INFO;

    static {
        String name = "Hekate";
        String ver = "UNKNOWN";
        String date = "UNKNOWN";
        String rev = "UNKNOWN";

        try (InputStream in = HekateVersion.class.getClassLoader().getResourceAsStream("hekate.properties")) {
            if (in != null) {
                Properties props = new Properties();

                props.load(in);

                name = props.getProperty("hekate.name", name);
                ver = props.getProperty("hekate.version", ver);
                date = props.getProperty("hekate.build.date", date);
                rev = props.getProperty("hekate.git.revision", date);
            }
        } catch (IOException e) {
            // No-op.
        }

        PROD_NAME = name.trim();
        VER_SHORT = ver.trim();
        VER_FULL = PROD_NAME + " " + VER_SHORT;
        DATE = date.trim();
        REV = rev.trim();

        INFO = VER_FULL + " (build: " + DATE + ", revision: " + REV + ")";
    }

    private HekateVersion() {
        // No-op.
    }

    /**
     * Returns detailed information about this version.
     *
     * @return Detailed version information.
     */
    public static String info() {
        return INFO;
    }

    /**
     * Returns the product name.
     *
     * @return Product name.
     */
    public static String productName() {
        return PROD_NAME;
    }

    /**
     * Returns the short version without the {@link #productName() product name} (x.y.z).
     *
     * @return Short version.
     */
    public static String shortVersion() {
        return VER_SHORT;
    }

    /**
     * Returns the full version number including the {@link #productName() product name}.
     *
     * @return Full version.
     */
    public static String fullVersion() {
        return VER_FULL;
    }

    /**
     * Returns the build date string (dd.MM.yyyy).
     *
     * @return Build date.
     */
    public static String date() {
        return DATE;
    }

    /**
     * Returns GIT commit id.
     *
     * @return Commit id.
     */
    public static String revision() {
        return REV;
    }
}
