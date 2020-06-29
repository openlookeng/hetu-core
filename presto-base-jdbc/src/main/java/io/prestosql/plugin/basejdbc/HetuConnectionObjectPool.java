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

import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Connection;

public class HetuConnectionObjectPool
        extends GenericObjectPool<Connection>
{
    private ConnectionPoolFactory factory;

    public HetuConnectionObjectPool(ConnectionPoolFactory factory, GenericObjectPoolConfig config, AbandonedConfig abandonedConfig)
    {
        super(factory, config, abandonedConfig);
        this.factory = factory;
    }

    public Connection borrowObject()
            throws Exception
    {
        factory.setForReturnConnection(this);
        Connection con = super.borrowObject();
        return con;
    }
}
