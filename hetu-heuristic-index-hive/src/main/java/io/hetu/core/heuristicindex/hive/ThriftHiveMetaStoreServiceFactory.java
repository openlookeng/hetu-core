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

package io.hetu.core.heuristicindex.hive;

import java.util.Properties;

/**
 * Factory for ThriftHiveMetaStoreService
 */
public class ThriftHiveMetaStoreServiceFactory
{
    private static ThriftHiveMetaStoreServiceFactory instance = new ThriftHiveMetaStoreServiceFactory();

    /**
     * Constructor for ThriftHiveMetaStoreServiceFactory
     */
    private ThriftHiveMetaStoreServiceFactory()
    {
    }

    /**
     * Returns instance of ThriftHiveMetaStoreServiceFactory
     *
     * @return instance - ThriftHiveMetaStoreServiceFactory
     */
    public static ThriftHiveMetaStoreServiceFactory getInstance()
    {
        return instance;
    }

    /**
     * Sets instance of ThriftHiveMetaStoreServiceFactory
     *
     * @param factory - factory for building ThriftHiveMetaStoreService
     */
    public static final void setInstance(ThriftHiveMetaStoreServiceFactory factory)
    {
        instance = factory;
    }

    /**
     * Returns ThriftHiveMetaStoreService
     *
     * @param properties Properties object that is usually the hive.properties
     * @return metaStoreService for thrift
     */
    public ThriftHiveMetaStoreService getMetaStoreService(Properties properties)
    {
        return new ThriftHiveMetaStoreService(properties);
    }
}
