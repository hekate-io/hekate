/*
 * Copyright 2018 The Hekate Project
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

import com.mysql.cj.jdbc.MysqlDataSource;
import io.hekate.HekateTestProps;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.postgresql.ds.PGSimpleDataSource;

public final class JdbcTestDataSources {
    private JdbcTestDataSources() {
        // No-op.
    }

    public static List<DataSource> all() {
        List<DataSource> dataSources = new ArrayList<>();

        dataSources.add(h2());

        // MySQL
        if (HekateTestProps.is("MYSQL_ENABLED")) {
            dataSources.add(mysql());
        }

        // PostgreSQL
        if (HekateTestProps.is("POSTGRES_ENABLED")) {
            dataSources.add(postgres());
        }

        return dataSources;

    }

    public static DataSource h2() {
        JdbcDataSource h2 = new JdbcDataSource();

        h2.setURL("jdbc:h2:mem:test");

        return h2;
    }

    public static DataSource postgres() {
        PGSimpleDataSource postgres = new PGSimpleDataSource();

        postgres.setUrl(HekateTestProps.get("POSTGRES_URL"));
        postgres.setUser(HekateTestProps.get("POSTGRES_USER"));
        postgres.setPassword(HekateTestProps.get("POSTGRES_PASSWORD"));

        return postgres;
    }

    public static DataSource mysql() {
        MysqlDataSource mysql = new MysqlDataSource();

        mysql.setURL(HekateTestProps.get("MYSQL_URL"));
        mysql.setUser(HekateTestProps.get("MYSQL_USER"));
        mysql.setPassword(HekateTestProps.get("MYSQL_PASSWORD"));

        return mysql;
    }
}
