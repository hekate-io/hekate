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

import io.hekate.HekateTestBase;
import io.hekate.test.HekateTestError;
import io.hekate.test.JdbcTestDataSources;
import io.hekate.util.format.ToString;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.Test;
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JdbcConnectivityDetectorTest extends HekateTestBase {
    @Test
    public void testRealDataSources() throws Exception {
        for (DataSource ds : JdbcTestDataSources.all()) {
            assertTrue(ds.toString(), new JdbcConnectivityDetector(ds, 3).isValid(newNode()));
        }
    }

    @Test
    public void testIsValidTrue() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.isValid(100500)).thenReturn(true);

        JdbcConnectivityDetector detector = new JdbcConnectivityDetector(ds, 100500);

        assertTrue(detector.isValid(newNode()));

        InOrder order = inOrder(ds, conn);

        order.verify(ds).getConnection();
        order.verify(conn).isValid(100500);
        order.verify(conn).close();
        verifyNoMoreInteractions(ds, conn);
    }

    @Test
    public void testIsValidFalse() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.isValid(100500)).thenReturn(false);

        JdbcConnectivityDetector detector = new JdbcConnectivityDetector(ds, 100500);

        assertFalse(detector.isValid(newNode()));

        InOrder order = inOrder(ds, conn);

        order.verify(ds).getConnection();
        order.verify(conn).isValid(100500);
        order.verify(conn).close();
        verifyNoMoreInteractions(ds, conn);
    }

    @Test
    public void testIsValidError() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection conn = mock(Connection.class);

        when(ds.getConnection()).thenReturn(conn);
        when(conn.isValid(100500)).thenThrow(new SQLException(HekateTestError.MESSAGE));

        JdbcConnectivityDetector detector = new JdbcConnectivityDetector(ds, 100500);

        assertFalse(detector.isValid(newNode()));

        InOrder order = inOrder(ds, conn);

        order.verify(ds).getConnection();
        order.verify(conn).isValid(100500);
        order.verify(conn).close();
        verifyNoMoreInteractions(ds, conn);
    }

    @Test
    public void testGetConnectionError() throws Exception {
        DataSource ds = mock(DataSource.class);

        when(ds.getConnection()).thenThrow(SQLException.class);

        JdbcConnectivityDetector detector = new JdbcConnectivityDetector(ds, 1);

        assertFalse(detector.isValid(newNode()));

        InOrder order = inOrder(ds);

        order.verify(ds).getConnection();
        verifyNoMoreInteractions(ds);
    }

    @Test
    public void testToString() {
        JdbcConnectivityDetector detector = new JdbcConnectivityDetector(mock(DataSource.class), 1);

        assertEquals(ToString.format(detector), detector.toString());
    }
}
