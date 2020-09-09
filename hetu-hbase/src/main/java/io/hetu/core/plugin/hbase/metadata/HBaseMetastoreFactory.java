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
package io.hetu.core.plugin.hbase.metadata;

import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorFactory;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.metastore.HetuMetastore;

/**
 * HBaseMetastoreFactory
 *
 * @since 2020-03-30
 */
public class HBaseMetastoreFactory
{
    private static final String TYPE_HETUMETASTORE = "hetuMetastore";
    private final HBaseConfig hbaseConfig;
    private final HetuMetastore hetuMetastore;

    public HBaseMetastoreFactory(HBaseConfig config)
    {
        this.hbaseConfig = config;
        this.hetuMetastore = HBaseConnectorFactory.getHetuMetastore();
    }

    /**
     * create hbase metastore
     *
     * @return HBaseMetastore
     */
    public HBaseMetastore create()
    {
        String type = hbaseConfig.getMetastoreType();
        if (type == null || TYPE_HETUMETASTORE.equals(type)) {
            return new HetuHBaseMetastore(hetuMetastore);
        }
        else {
            throw new PrestoException(HBaseErrorCode.UNSUPPORTED_TYPE, "Unsupported metastore type");
        }
    }
}
