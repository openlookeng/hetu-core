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
package io.prestosql.plugin.hive;

import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

public class LegacyHiveViewReaderTest
{
    private LegacyHiveViewReader legacyHiveViewReaderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        legacyHiveViewReaderUnderTest = new LegacyHiveViewReader();
    }

    @Test
    public void testDecodeViewData()
    {
        // Setup
        final Table table = new Table("databaseName", "tableName", "owner", "tableType", new Storage(
                StorageFormat.create("serde", "inputFormat", "outputFormat"), "location",
                Optional.of(new HiveBucketProperty(
                        Arrays.asList("value"), HiveBucketing.BucketingVersion.BUCKETING_V1, 0,
                        Arrays.asList(new SortingColumn("columnName", SortingColumn.Order.ASCENDING)))), false,
                new HashMap<>()),
                Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                Arrays.asList(new Column("name", HiveType.HIVE_BYTE, Optional.of("value"))),
                new HashMap<>(), Optional.of("value"), Optional.of("value"));
        final CatalogName catalogName = new CatalogName("catalogName");

        // Run the test
        final ConnectorViewDefinition result = legacyHiveViewReaderUnderTest.decodeViewData("viewData", table,
                catalogName);

        // Verify the results
    }
}
