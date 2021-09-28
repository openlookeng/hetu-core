/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * TestHBaseMetastoreFactory
 *
 * @since 2020-03-20
 */
public class TestHBaseMetastoreFactory
{
    /**
     * testHBaseMetastoreFactoryHetuMetastore
     */
    @Test(expectedExceptions = NullPointerException.class)
    public void testHBaseMetastoreFactoryHetuMetastore()
    {
        HBaseConfig hBConf = new HBaseConfig();
        hBConf.setMetastoreType("hetuMetastore");
        HBaseMetastoreFactory hBMetaFactory = new HBaseMetastoreFactory(hBConf);
        hBMetaFactory.create();
    }

    /**
     * testHBaseMetastoreFactoryOthers
     */
    @Test
    public void testHBaseMetastoreFactoryOthers()
    {
        HBaseConfig hBConf = new HBaseConfig();
        hBConf.setMetastoreType("others");
        HBaseMetastoreFactory hBMetaFactory = new HBaseMetastoreFactory(hBConf);
        try {
            hBMetaFactory.create();
            throw new PrestoException(HBaseErrorCode.UNSUPPORTED_TYPE, "testHBaseMetastoreFactory -> Others : failed");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), "Unsupported metastore type");
        }
    }

    /**
     * testConnectorTableMetadata
     */
    @Test
    public void testConnectorTableMetadata()
    {
        ConnectorTableMetadata ctm =
                new ConnectorTableMetadata(
                        new SchemaTableName("hbase", "newTable"), TestUtils.createColumnMetadataList());
        ctm.getComment();
        ctm.toString();
    }
}
