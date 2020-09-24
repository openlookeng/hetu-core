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
package io.hetu.core.plugin.heuristicindex.datasource.hive;

import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestHdfsOrcDataSource
{
    @Test
    public void testConcurrencySetting()
    {
        Integer concurrency = 30;

        Properties properties = new Properties();
        properties.setProperty(ConstantsHelper.HDFS_SOURCE_CONCURRENCY, concurrency.toString());

        HdfsOrcDataSource source = new HdfsOrcDataSource();
        source.setProperties(properties);

        assertEquals(source.getConcurrency(), concurrency.intValue());
    }

    @Test
    public void testConcurrencySettingException()
    {
        Integer concurrency = -30;

        Properties properties = new Properties();
        properties.setProperty(ConstantsHelper.HDFS_SOURCE_CONCURRENCY, concurrency.toString());

        HdfsOrcDataSource source = new HdfsOrcDataSource();
        source.setProperties(properties);

        // has to call this since it's lazy init
        assertThrows(RuntimeException.class, source::getConcurrency);
        assertThrows(RuntimeException.class, () -> source.setConcurrency(0));
        assertThrows(RuntimeException.class, () -> source.setConcurrency(-1));
    }

    @Test
    public void testEmptyConcurrencySetting()
    {
        Properties properties = new Properties();

        HdfsOrcDataSource source = new HdfsOrcDataSource();
        source.setProperties(properties);

        assertEquals(source.getConcurrency(), TestConstantsHelper.DEFAULT_CONCURRENCY);
    }

    @Test
    public void testValidatePartitions()
    {
        TableMetadata tableMetadata = getMockTableMetadata();

        // valid
        HdfsOrcDataSource.validatePartitions(
                new String[] {"partitioncolumn=1", "partitioncolumn=2"},
                tableMetadata);

        HdfsOrcDataSource.validatePartitions(
                new String[] {"partitioncolumn=1"},
                tableMetadata);

        HdfsOrcDataSource.validatePartitions(
                new String[] {"PARTITIONCOLUMN=1"},
                tableMetadata);

        // invalid
        assertThrows(IllegalArgumentException.class, () -> HdfsOrcDataSource.validatePartitions(
                new String[] {"invalidpartitioncolumn=1", "partitioncolumn=2" },
                tableMetadata));

        assertThrows(IllegalArgumentException.class, () -> HdfsOrcDataSource.validatePartitions(
                new String[] {"invalidpartitioncolumn=1"},
                tableMetadata));

        assertThrows(IllegalArgumentException.class, () -> HdfsOrcDataSource.validatePartitions(
                new String[] {"partitioncolumn1", "partitioncolumn=2"},
                tableMetadata));

        assertThrows(IllegalArgumentException.class, () -> HdfsOrcDataSource.validatePartitions(
                new String[] {"partitioncolumn1"},
                tableMetadata));
    }

    private TableMetadata getMockTableMetadata()
    {
        TableMetadata tableMetadata = mock(TableMetadata.class);
        Map<HiveColumnHandle, Type> validPartitions = new HashMap<>();
        HiveColumnHandle columnHandle = mock(HiveColumnHandle.class);
        when(columnHandle.getName()).thenReturn("partitionColumn");
        Type columnType = mock(Type.class);
        validPartitions.put(columnHandle, columnType);
        when(tableMetadata.getPartitionColumns()).thenReturn(validPartitions);

        return tableMetadata;
    }
}
