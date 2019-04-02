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

package io.hekate.cluster.split;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.util.format.ToString;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC connectivity-based detector.
 *
 * <p>
 * This implementation of {@link SplitBrainDetector} utilizes {@link Connection#isValid(int)} method to check if some pre-configured
 * JDBC database is reachable.
 * </p>
 *
 * <p>
 * Note that it is possible to combine multiple detectors with the help of {@link SplitBrainDetectorGroup}.
 * </p>
 *
 * @see ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector)
 */

public class JdbcConnectivityDetector implements SplitBrainDetector {
    private static final Logger log = LoggerFactory.getLogger(JdbcConnectivityDetector.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final int timeout;

    private final DataSource ds;

    /**
     * Constructs a new instance.
     *
     * @param ds Data source.
     * @param timeout Timeout in seconds (see {@link Connection#isValid(int)}).
     */
    public JdbcConnectivityDetector(DataSource ds, int timeout) {
        ConfigCheck check = ConfigCheck.get(JdbcConnectivityDetector.class);

        check.notNull(ds, "data source");
        check.positive(timeout, "timeout");

        ArgAssert.notNull(ds, "Data source");

        this.ds = ds;
        this.timeout = timeout;
    }

    @Override
    public boolean isValid(ClusterNode localNode) {
        try (Connection conn = ds.getConnection()) {
            boolean valid = conn.isValid(timeout);

            if (valid) {
                if (DEBUG) {
                    log.debug("JDBC reachability check successful [datasource={}]", ds);
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("JDBC reachability check failed [datasource={}, timeout={}]", ds, timeout);
                }
            }

            return valid;
        } catch (SQLException e) {
            if (log.isWarnEnabled()) {
                log.warn("JDBC reachability check failed [datasource={}, timeout={}, cause={}]", ds, timeout, e.toString());
            }

            return false;
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
