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

import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.RetryMode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.iceberg.BaseTable;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;

public class TableStatisticsMakerTest
{
    private TableStatisticsMaker tableStatisticsMakerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        tableStatisticsMakerUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testGetTableStatistics()
    {
        // Setup
        final TypeManager typeManager = null;
        final Constraint constraint = new Constraint(TupleDomain.withColumnDomains(new HashMap<>()), val -> {
            return false;
        });
        final IcebergTableHandle tableHandle = new IcebergTableHandle(
                "schemaName",
                "tableName",
                TableType.DATA,
                Optional.of(0L),
                "tableSchemaJson",
                "partitionSpecJson",
                1,
                TupleDomain.withColumnDomains(new HashMap<>()),
                TupleDomain.withColumnDomains(new HashMap<>()),
                new HashSet<>(Arrays.asList(new IcebergColumnHandle(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new IcebergTableHandleTest.Type(), Arrays.asList(0), new IcebergTableHandleTest.Type(), Optional.of("value")))),
                Optional.of("value"),
                "tableLocation",
                new HashMap<>(),
                RetryMode.RETRIES_ENABLED,
                Arrays.asList(new IcebergColumnHandle(new ColumnIdentity(0, "name", ColumnIdentity.TypeCategory.PRIMITIVE, Arrays.asList()), new IcebergTableHandleTest.Type(), Arrays.asList(0), new IcebergTableHandleTest.Type(), Optional.of("value"))));
        BaseTable table = new BaseTable(new HistoryTableTest.TableOperations(), "name");

        // Run the test
        TableStatisticsMaker.getTableStatistics(typeManager, constraint, tableHandle, table);
    }
}
