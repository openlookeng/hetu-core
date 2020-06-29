/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.plugin.basejdbc;

import io.airlift.log.Logger;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkState;

public class ConnectionPoolFactory
        implements PooledObjectFactory<Connection>
{
    private static final Logger LOG = Logger.get(ConnectionPoolFactory.class);
    private final Driver driver;
    private final String connectionUrl;
    private final Properties connectionProperties;
    private HetuConnectionObjectPool forReturnConnection;
    private ConcurrentHashMap<Connection, HetuConnectionHandler> connectionHandlers = new ConcurrentHashMap<>();

    public ConnectionPoolFactory(
            Driver driver,
            String connectionUrl,
            Properties connectionProperties)
    {
        this.driver = driver;
        this.connectionUrl = connectionUrl;
        this.connectionProperties = connectionProperties;
    }

    public void setForReturnConnection(HetuConnectionObjectPool forReturnConnection)
    {
        this.forReturnConnection = forReturnConnection;
    }

    @Override
    public PooledObject<Connection> makeObject()
            throws Exception
    {
        return this.wrap(this.create());
    }

    @Override
    public void destroyObject(PooledObject<Connection> p)
            throws SQLException
    {
        Connection con = p.getObject();
        if (con != null) {
            HetuConnectionHandler proxy = connectionHandlers.get(con);
            proxy.closeConnection();
            connectionHandlers.remove(con);
            if (LOG.isDebugEnabled()) {
                LOG.debug("destroyObject: connection close...connections[%s]", con.toString());
            }
        }
    }

    @Override
    public boolean validateObject(PooledObject<Connection> p)
    {
        Connection con = p.getObject();
        if (con != null) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("validateObject: valid connecion[%s]' valid=%s", con.toString(), con.isValid(2));
                }
                return con.isValid(1);
            }
            catch (SQLException e) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("validateObject: valid connection failed..cause by %s", e.getMessage());
                }
            }
        }
        return false;
    }

    @Override
    public void activateObject(PooledObject<Connection> p)
            throws SQLException
    {
        // do nothing
    }

    @Override
    public void passivateObject(PooledObject<Connection> p)
            throws SQLException
    {
        // do nothing
    }

    public Connection create()
            throws SQLException
    {
        Connection connection = null;
        try {
            connection = driver.connect(connectionUrl, connectionProperties);
            HetuConnectionHandler proxy = new HetuConnectionHandler();
            connection = proxy.bind(connection, this.forReturnConnection);
            connectionHandlers.put(connection, proxy);
        }
        catch (Exception e) {
            LOG.error("create: create Connection failed... cause by ", e);
        }
        checkState(connection != null, "Driver returned null connection");
        if (LOG.isDebugEnabled()) {
            LOG.debug("create(): create Connection success... connection[%s]", connection);
        }
        return connection;
    }

    public PooledObject<Connection> wrap(Connection connection)
    {
        return new DefaultPooledObject<Connection>(connection);
    }
}
