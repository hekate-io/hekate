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

import io.hekate.HekateTestProps;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

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
        return new DataSourceMock("jdbc:h2:mem:test", null, null);
    }

    public static DataSource postgres() {
        String url = HekateTestProps.get("POSTGRES_URL");
        String user = HekateTestProps.get("POSTGRES_USER");
        String password = HekateTestProps.get("POSTGRES_PASSWORD");

        return new DataSourceMock(url, user, password);
    }

    public static DataSource mysql() {
        String url = HekateTestProps.get("MYSQL_URL");
        String user = HekateTestProps.get("MYSQL_USER");
        String password = HekateTestProps.get("MYSQL_PASSWORD");

        url = user.contains("?") ? url + "&useSSL=false" : url + "?useSSL=false";

        return new DataSourceMock(url, user, password);
    }
}
