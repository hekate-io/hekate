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

package io.hekate.cluster.seed.jdbc;

import io.hekate.core.jmx.JmxTypeName;
import javax.management.MXBean;
import javax.sql.DataSource;

/**
 * JMX interface for {@link JdbcSeedNodeProvider}.
 */
@MXBean
@JmxTypeName("JdbcSeedNodeProvider")
public interface JdbcSeedNodeProviderJmx {
    /**
     * Returns the string representation of {@link JdbcSeedNodeProviderConfig#setDataSource(DataSource)}.
     *
     * @return String representation of {@link JdbcSeedNodeProviderConfig#setDataSource(DataSource)}.
     */
    String getDataSourceInfo();

    /**
     * Returns the value of {@link JdbcSeedNodeProviderConfig#setQueryTimeout(int)}.
     *
     * @return Value of {@link JdbcSeedNodeProviderConfig#setQueryTimeout(int)}.
     */
    int getQueryTimeout();

    /**
     * Returns the value of {@link JdbcSeedNodeProviderConfig#setCleanupInterval(long)}.
     *
     * @return Value of {@link JdbcSeedNodeProviderConfig#setCleanupInterval(long)}.
     */
    long getCleanupInterval();

    /**
     * Returns the SQL statement for inserting data into the {@link JdbcSeedNodeProviderConfig#setTable(String) seed nodes table}.
     *
     * @return SQL statement for inserting data into the {@link JdbcSeedNodeProviderConfig#setTable(String) seed nodes table}.
     */
    String getInsertSql();

    /**
     * Returns the SQL statement for selecting data from the {@link JdbcSeedNodeProviderConfig#setTable(String) seed nodes table}.
     *
     * @return SQL statement for selecting data from the {@link JdbcSeedNodeProviderConfig#setTable(String) seed nodes table}.
     */
    String getSelectSql();

    /**
     * Returns the SQL statement for deleting data from the {@link JdbcSeedNodeProviderConfig#setTable(String) seed nodes table}.
     *
     * @return SQL statement for deleting data from the {@link JdbcSeedNodeProviderConfig#setTable(String) seed nodes table}.
     */
    String getDeleteSql();
}
