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

package io.hekate.metrics.influxdb;

import io.hekate.HekateNodeTestBase;
import io.hekate.HekateTestProps;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;

import static io.hekate.metrics.influxdb.InfluxDbMetricsPlugin.METRIC_VALUE_FIELD;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class InfluxDbMetricsTestBase extends HekateNodeTestBase {
    protected static String url;

    protected static String database;

    protected static String user;

    protected static String password;

    protected static InfluxDB influxDb;

    @BeforeClass
    public static void prepareTestClass() {
        // May be disable the whole test class.
        Assume.assumeTrue(HekateTestProps.is("INFLUXDB_ENABLED"));

        url = HekateTestProps.get("INFLUXDB_URL");
        user = HekateTestProps.get("INFLUXDB_USER");
        password = HekateTestProps.get("INFLUXDB_PASSWORD");

        String now = new SimpleDateFormat("yyyy.MM.dd--HH:mm:SSS").format(new Date());

        database = InfluxDbMetricsPublisher.toSafeName("db__" + now + "__" + UUID.randomUUID().toString());

        influxDb = InfluxDBFactory.connect(url, user, password);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        say("Testing with InfluxDB database: " + database);

        influxDb.deleteDatabase(database);
        influxDb.createDatabase(database);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        try {
            influxDb.deleteDatabase(database);
        } finally {
            super.tearDown();
        }
    }

    protected Long getLatestValue(String metric) {
        Long loaded = loadLatestValue(metric);

        assertNotNull(metric, loaded);

        return loaded;
    }

    protected Long loadLatestValue(String metric) {
        Query query = new Query("SELECT " + METRIC_VALUE_FIELD + " FROM \"" + metric + "\" ORDER BY time DESC LIMIT 1", database);

        QueryResult result = influxDb.query(query);

        assertNull(result.getError());
        assertFalse(result.hasError());

        QueryResult.Result row = result.getResults().get(0);

        say("Loaded InfluxDB results: " + row);

        Number number = null;

        if (row.getSeries() != null && !row.getSeries().isEmpty()) {
            QueryResult.Series series = row.getSeries().get(0);

            int valueIdx = series.getColumns().indexOf(METRIC_VALUE_FIELD);

            assertTrue("Real index=" + valueIdx, valueIdx >= 0);

            number = (Number)series.getValues().get(0).get(valueIdx);
        }

        return number != null ? number.longValue() : null;
    }
}
