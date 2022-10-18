/*
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
package io.hetu.core.plugin.iceberg;

import io.prestosql.spi.connector.RetryMode;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class IcebergWritableTableHandleTest
{
    private IcebergWritableTableHandle icebergWritableTableHandleUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergWritableTableHandleUnderTest = new IcebergWritableTableHandle("schemaName", "tableName", "schemaAsJson",
                "partitionSpecAsJson",
                Arrays.asList(new IcebergColumnHandle(
                        new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), BIGINT,
                        Arrays.asList(0), BIGINT, Optional.of("value"))), "outputPath", IcebergFileFormat.ORC,
                new HashMap<>(),
                RetryMode.NO_RETRIES);
    }

    @Test
    public void test()
    {
        icebergWritableTableHandleUnderTest.getSchemaName();
        icebergWritableTableHandleUnderTest.getTableName();
        icebergWritableTableHandleUnderTest.getSchemaAsJson();
        icebergWritableTableHandleUnderTest.getPartitionSpecAsJson();
        icebergWritableTableHandleUnderTest.getInputColumns();
        icebergWritableTableHandleUnderTest.getOutputPath();
        icebergWritableTableHandleUnderTest.getFileFormat();
        icebergWritableTableHandleUnderTest.getStorageProperties();
        icebergWritableTableHandleUnderTest.getRetryMode();
    }

    @Test
    public void testToString()
    {
        icebergWritableTableHandleUnderTest.toString();
    }
}
