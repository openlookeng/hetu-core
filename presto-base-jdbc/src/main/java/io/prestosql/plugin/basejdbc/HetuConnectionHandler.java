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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;

public class HetuConnectionHandler
        implements InvocationHandler
{
    private static final Logger LOG = Logger.get(HetuConnectionHandler.class);
    private Connection realConnection;
    private Connection wrapedConnection;
    private HetuConnectionObjectPool pool;

    Connection bind(Connection realConnection, HetuConnectionObjectPool pool)
    {
        this.pool = pool;
        this.realConnection = realConnection;
        this.wrapedConnection = (Connection) Proxy.newProxyInstance(this.getClass().getClassLoader(),
                new Class[] {Connection.class}, this);
        return wrapedConnection;
    }

    public void closeConnection()
    {
        try {
            this.realConnection.close();
            if (LOG.isDebugEnabled()) {
                LOG.debug("closeConnection: close realConnection success...");
            }
        }
        catch (Exception e) {
            LOG.error("closeConnection failed... cause by ", e);
        }
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("invoke: invoke connection method[%s]...connection is valide=%s", method.getName(), this.realConnection.isValid(1));
        }

        if ("close".equals(method.getName())) {
            // return connection, do not close realConnection
            if (LOG.isDebugEnabled()) {
                LOG.debug("invoke: instead of close connection, return conection to pool...wrapped-connection[%s]", this.wrapedConnection);
            }

            if (this.realConnection.isClosed()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("invoke: close the connection because the connection is already closed...real-connection[%s]", this.realConnection);
                }
                this.closeConnection();
            }
            else {
                this.pool.returnObject(this.wrapedConnection);
            }

            return null;
        }
        else {
            try {
                return method.invoke(this.realConnection, args);
            }
            catch (InvocationTargetException e) {
                throw e.getTargetException();
            }
        }
    }
}
