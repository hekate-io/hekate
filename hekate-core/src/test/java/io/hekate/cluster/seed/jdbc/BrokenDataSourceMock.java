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

import io.hekate.test.HekateTestError;
import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;

public class BrokenDataSourceMock implements DataSource {
    private final DataSource ds;

    private final AtomicInteger errors = new AtomicInteger();

    public BrokenDataSourceMock(DataSource ds) {
        this.ds = ds;
    }

    public void scheduleErrors(int amount) {
        errors.set(amount);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection conn = ds.getConnection();

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class<?>[] interfaces = new Class[]{Connection.class};

        return (Connection)Proxy.newProxyInstance(classLoader, interfaces, (proxy, method, args) -> {
            if (method.getName().toLowerCase().contains("statement")) {
                if (errors.get() > 0 && errors.getAndDecrement() > 0) {
                    throw new SQLException(HekateTestError.MESSAGE);
                }
            }

            return method.invoke(conn, args);
        });
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return ds.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return ds.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        ds.setLogWriter(out);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return ds.getLoginTimeout();
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        ds.setLoginTimeout(seconds);
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ds.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> type) throws SQLException {
        return ds.unwrap(type);
    }

    @Override
    public boolean isWrapperFor(Class<?> type) throws SQLException {
        return ds.isWrapperFor(type);
    }
}
