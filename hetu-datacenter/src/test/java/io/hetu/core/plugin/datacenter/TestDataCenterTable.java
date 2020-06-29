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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnMetadata;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestDataCenterTable
{
    private final DataCenterTable dataCenterTable = new DataCenterTable("tableName",
            ImmutableList.of(new DataCenterColumn("a", createUnboundedVarcharType()), new DataCenterColumn("b", BIGINT)));

    @Test
    public void testColumnMetadata()
    {
        assertEquals(dataCenterTable.getColumnsMetadata(),
                ImmutableList.of(new ColumnMetadata("a", createUnboundedVarcharType()), new ColumnMetadata("b", BIGINT)));
    }

    @Test
    public void testRoundTrip()
    {
        String json = MetadataUtil.TABLE_CODEC.toJson(dataCenterTable);
        DataCenterTable dataCenterTableCopy = MetadataUtil.TABLE_CODEC.fromJson(json);

        assertEquals(dataCenterTableCopy.getName(), dataCenterTable.getName());
        assertEquals(dataCenterTableCopy.getColumns(), dataCenterTable.getColumns());
    }
}
