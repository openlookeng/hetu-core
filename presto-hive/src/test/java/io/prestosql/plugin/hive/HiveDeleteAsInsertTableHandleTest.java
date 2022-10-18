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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.metastore.HivePageSinkMetadata;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class HiveDeleteAsInsertTableHandleTest
{
    @Mock
    private HivePageSinkMetadata mockPageSinkMetadata;
    @Mock
    private LocationHandle mockLocationHandle;

    private HiveDeleteAsInsertTableHandle hiveDeleteAsInsertTableHandleUnderTest;

    @Test
    public void setUp() throws Exception
    {
        initMocks(this);
        hiveDeleteAsInsertTableHandleUnderTest = new HiveDeleteAsInsertTableHandle("schemaName", "tableName",
                Arrays.asList(new HiveColumnHandle("name", HiveType.HIVE_BOOLEAN,
                        new TypeSignature("base", TypeSignatureParameter.of(0L)), 0,
                        HiveColumnHandle.ColumnType.PARTITION_KEY, Optional.of("value"), false)), mockPageSinkMetadata,
                mockLocationHandle,
                Optional.of(
                        new HiveBucketProperty(Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                                Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))),
                HiveStorageFormat.ORC, HiveStorageFormat.ORC);
    }
}
